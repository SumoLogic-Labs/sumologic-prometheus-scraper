#!/usr/bin/env python

import asyncio
import click
import concurrent.futures
import functools
import gzip
import json
import logging
import os
import re
import fnmatch
import requests
import math
import time
import urllib3

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
from itertools import islice
from json.decoder import JSONDecodeError
from prometheus_client.parser import text_string_to_metric_families
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from voluptuous import (
    ALLOW_EXTRA,
    All,
    Any,
    Boolean,
    IsFile,
    Length,
    Invalid,
    MultipleInvalid,
    Optional,
    Or,
    Range,
    Required,
    Schema,
    Url,
)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging_level = os.environ.get("LOGGING_LEVEL", "ERROR")
logging_format = "%(asctime)s [level=%(levelname)s] [thread=%(threadName)s] [module=%(module)s] [line=%(lineno)d]: %(message)s"
logging.basicConfig(level=logging_level, format=logging_format)
log = logging.getLogger(__name__)
scheduler = BlockingScheduler(timezone="UTC")
monitors = {}


def batches(iterator, batch_size: int):
    """ Yields lists of max batch_size from given iterator"""
    while True:
        batch = list(islice(iterator, batch_size))
        if not batch:
            break
        yield batch


def sanitize_labels(labels: dict):
    """Given prometheus metric sample labels, returns labels dict suitable for Prometheus format"""
    new_labels = ""
    for key, value in labels.items():
        new_labels += "=".join([key, '"' + value + '"', ","])
    return f"{{{new_labels[:-2]}}}"


def match_regexp(glob_list: list, default: str):
    """Converts a list of glob matches into a single compiled regexp
    If list is empty, a compilation of default regexp is returned instead"""
    if not glob_list:
        return re.compile(default)
    return re.compile(r"|".join(fnmatch.translate(p) for p in glob_list))


class SumoHTTPAdapter(HTTPAdapter):
    CONFIG_TO_HEADER = {
        "source_category": "X-Sumo-Category",
        "source_name": "X-Sumo-Name",
        "source_host": "X-Sumo-Host",
        "metadata": "X-Sumo-Metadata",
    }

    def __init__(self, config, max_retries, **kwds):
        self._prepared_headers = self._prepare_headers(config)
        super().__init__(max_retries=max_retries, **kwds)

    def add_headers(self, request, **kwds):
        for k, v in self._prepared_headers.items():
            request.headers[k] = v

    def _prepare_headers(self, config):
        headers = {}
        for config_key, header_name in self.CONFIG_TO_HEADER.items():
            if config_key in config:
                headers[header_name] = config[config_key]

        dimensions = f"job={config['name']},instance={config['url']}"
        if "dimensions" in config:
            dimensions += f",{config['dimensions']}"
        headers["X-Sumo-Dimensions"] = dimensions
        return headers


class SumoPrometheusScraperConfig:
    def __init__(self):
        self.global_config_schema = Schema(
            {
                Optional("sumo_http_url"): Url(),
                Required("run_interval_seconds", default=60): All(int, Range(min=1)),
                Required("target_threads", default=10): All(int, Range(min=1, max=50)),
                Required("batch_size", default=1000): All(int, Range(min=1)),
                Required("retries", default=5): All(int, Range(min=1, max=20)),
                Required("backoff_factor", default=0.2): All(float, Range(min=0)),
                "source_category": str,
                "source_host": str,
                "source_name": str,
                "dimensions": str,
                "metadata": str,
            }
        )

        self.target_source_config = Schema(
            Or(
                {Required("url"): Url()},
                {Required("service"): str, Required("namespace"): str},
            )
        )

        url_schema = Schema(
            Or(
                Required("url"),
                Url(),
                {
                    Required("service"): str,
                    Required("namespace"): str,
                    Required("path", default="/metrics"): str,
                    Required("protocol", default="http"): str,
                },
            )
        )

        self.target_config_schema = self.global_config_schema.extend(
            {
                Required("url", default={}): url_schema,
                Required("name"): str,
                Required("exclude_metrics", default=[]): list([str]),
                Required("include_metrics", default=[]): list([str]),
                Required("exclude_labels", default={}): Schema({}, extra=ALLOW_EXTRA),
                Required("include_labels", default={}): Schema({}, extra=ALLOW_EXTRA),
                Required("strip_labels", default=[]): list([str]),
                Required("should_callback", default=True): bool,
                "token_file_path": IsFile(),
                "verify": Any(Boolean(), str),
                # repeat keys from global to remove default values
                "sumo_http_url": Url(),
                "run_interval_seconds": All(int, Range(min=1)),
                "target_threads": All(int, Range(min=1, max=50)),
                "batch_size": All(int, Range(min=1)),
                "retries": All(int, Range(min=1, max=20)),
                "backoff_factor": All(float, Range(min=0)),
            }
        )

        self.config_schema = Schema(
            All(
                {
                    Required("global", default={}): self.global_config_schema,
                    Required("targets"): All(
                        Length(min=1, max=256), [self.target_config_schema]
                    ),
                },
                self.check_url,
            )
        )

    @staticmethod
    def check_url(config):
        if "global" in config:
            if "sumo_http_url" in config["global"]:
                return config

        for t in config["targets"]:
            if "sumo_http_url" not in t:
                raise Invalid("sumo_http_url must be set on target or global.")
        return config


class SumoPrometheusScraper:
    def __init__(self, name: str, config: dict, callback=None):
        self._config = config
        self._name = name
        self._should_callback = config["should_callback"]
        self._batch_size = config["batch_size"]
        self._sumo_session = None
        self._scrape_session = None
        self._exclude_metrics_re = match_regexp(
            self._config["exclude_metrics"], default=r"$."
        )
        self._include_metrics_re = match_regexp(
            self._config["include_metrics"], default=r".*"
        )
        self._exclude_labels = self._config["exclude_labels"]
        self._include_labels = self._config["include_labels"]
        self._strip_labels = self._config["strip_labels"]
        self._callback = callback
        if callback and callable(self._callback):
            self._callback = functools.partial(callback)

        retries = config["retries"]

        self._scrape_session = requests.Session()
        sumo_retry = Retry(
            total=retries,
            read=retries,
            method_whitelist=frozenset(["POST", *Retry.DEFAULT_METHOD_WHITELIST]),
            connect=retries,
            backoff_factor=config["backoff_factor"],
        )

        self._sumo_session = requests.Session()
        adapter = SumoHTTPAdapter(config, max_retries=sumo_retry)
        self._sumo_session.mount("http://", adapter)
        self._sumo_session.mount("https://", adapter)

        if "token_file_path" in self._config:
            with open(self._config["token_file_path"]) as f:
                token = f.read().strip()
            self._scrape_session.headers["Authorization"] = f"Bearer {token}"
        if "verify" in self._config:
            self._scrape_session.verify = self._config["verify"]

    def _parsed_samples(self, prometheus_metrics: str, scrape_ts: int):
        for metric_family in text_string_to_metric_families(prometheus_metrics):
            for sample in metric_family.samples:
                name, labels, value, ts, exemplar = sample
                if (
                    self._callback
                    and callable(self._callback)
                    and self._should_callback
                ):
                    name, labels, value = self._callback(name, labels, value)
                if math.isnan(value):
                    continue
                if (
                    self._include_metrics_re.match(name)
                    and not self._exclude_metrics_re.match(name)
                    and self._should_include(labels)
                    and not self._should_exclude(labels)
                ):
                    for label_key in self._strip_labels:
                        labels.pop(label_key, None)
                    yield f"{name}{sanitize_labels(labels)} {value} {scrape_ts}"

    def _should_include(self, labels):
        for key, value in self._include_labels.items():
            if key in labels and not re.match(value, labels[key]):
                return False
        return True

    def _should_exclude(self, labels):
        for key, value in self._exclude_labels.items():
            if key in labels and re.match(value, labels[key]):
                return True
        return False

    async def _post_to_sumo(self, resp, scrape_ts: int):
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self._config["target_threads"]
        ) as executor:
            event_loop = asyncio.get_event_loop()
            futures = [
                event_loop.run_in_executor(
                    executor, self._compress_and_send, batch
                )
                for batch in batches(self._parsed_samples(resp.text, scrape_ts), self._batch_size)
            ]
        for _ in await asyncio.gather(*futures):
            pass

    def _compress_and_send(self, batch):
        body = "\n".join(batch).encode("utf-8")
        try:
            resp = self._sumo_session.post(
                self._config["sumo_http_url"],
                data=gzip.compress(body, compresslevel=1),
                headers={
                    "Content-Type": "application/vnd.sumologic.prometheus",
                    "Content-Encoding": "gzip",
                    "X-Sumo-Client": "prometheus-scraper",
                },
            )
            resp.raise_for_status()
            log.info(
                f"posting batch to Sumo logic for {self._config['name']} took {resp.elapsed.total_seconds()} seconds"
            )
        except requests.exceptions.HTTPError as http_error:
            log.error(
                f"unable to send batch for {self._config['name']} to Sumo Logic, got back response {resp.status_code} and error {http_error}"
            )

    def run(self):
        start = int(time.time())
        try:
            resp = self._scrape_session.get(self._config["url"])
            resp.raise_for_status()
            scrape_ts = int(time.time())
            self.send_up(1)
            log.info(
                f"scrape of {self._config['name']} took {resp.elapsed.total_seconds()} seconds"
            )
            event_loop = asyncio.new_event_loop()
            event_loop.run_until_complete(self._post_to_sumo(resp, scrape_ts))

            log.info(
                f"total time taken for {self._config['name']} was {(time.time() - start)} seconds"
            )
        except requests.exceptions.HTTPError as http_error:
            self.send_up(0)
            log.error(
                f"unable to send batch for {self._config['name']} to Sumo Logic, got back response {resp.status_code} and error {http_error}"
            )

    def send_up(self, up):
        data = f"metric=up  {up} {int(time.time())}"
        resp = self._sumo_session.post(
            self._config["sumo_http_url"],
            data=data,
            headers={
                "Content-Type": "application/vnd.sumologic.carbon2",
                "X-Sumo-Client": "prometheus-scraper",
            },
        )
        log.debug(
            f"got back status code {resp.status_code} for up metric for {self._config['name']}"
        )
        resp.raise_for_status()


@scheduler.scheduled_job(
    "interval", id="synchronize", seconds=int(os.environ.get("SYNC_INTERVAL", "10"))
)
def synchronize():
    current_monitors = monitors.copy()
    for key, value in current_monitors.items():
        endpoint = lookup_endpoint(
            value["original_target_url"]["service"],
            value["original_target_url"]["namespace"],
        )
        if value["ip"] not in str(endpoint):
            log.debug(
                f"change to monitor {key} detected, ip {value['ip']} has been removed, will remove job"
            )
            scheduler.remove_job(key)
            del monitors[key]
        for subset in endpoint["subsets"]:
            for idx, address in enumerate(subset["addresses"]):
                for port in subset["ports"]:
                    url = f"{value['original_target_url']['protocol']}://{address['ip']}:{port['port']}{value['original_target_url']['path']}"
                    job_id = f"{value['original_target_name']}_{url}"
                    if not scheduler.get_job(job_id):
                        log.debug(
                            f"change to monitor {key} detected, ip {address['ip']} has been added, will add job"
                        )
                        new_target = value["target"].copy()
                        new_target["name"] = job_id
                        new_target["url"] = url
                        scraper = SumoPrometheusScraper(job_id, new_target)
                        scheduler.add_job(
                            func=scraper.run,
                            name=job_id,
                            id=job_id,
                            trigger="interval",
                            seconds=new_target["run_interval_seconds"],
                        )
                        create_monitor(
                            new_target,
                            address["ip"],
                            port["port"],
                            value["original_target_url"],
                            value["original_target_name"],
                        )
    log.debug(f"current monitors: {len(monitors)}")
    log.debug(f"current jobs: {len(scheduler.get_jobs())}")


def lookup_endpoint(service, namespace):
    headers = {}
    with open("/var/run/secrets/kubernetes.io/serviceaccount/token") as f:
        token = f.read().strip()
    headers["Authorization"] = f"Bearer {token}"
    resp = requests.get(
        url=f"https://kubernetes.default.svc/api/v1/namespaces/{namespace}/endpoints/{service}",
        verify="/run/secrets/kubernetes.io/serviceaccount/ca.crt",
        headers=headers,
    )
    resp.raise_for_status()
    return json.loads(resp.content)


def expand_config(config):
    pre_target_length = len(config["targets"])
    targets = []
    for target in config["targets"]:
        if isinstance(target["url"], dict):
            log.debug(f"target {target['name']} needs to be expanded")
            endpoint = lookup_endpoint(
                target["url"]["service"], target["url"]["namespace"]
            )
            for subset in endpoint["subsets"]:
                for idx, address in enumerate(subset["addresses"]):
                    for port in subset["ports"]:
                        new_target = target.copy()
                        url = f"{target['url']['protocol']}://{address['ip']}:{port['port']}{target['url']['path']}"
                        new_target["url"] = url
                        new_target["name"] = f"{new_target['name']}_{url}"
                        scheduler_config = {}
                        scheduler_config.update(new_target)
                        for k, v in config["global"].items():
                            scheduler_config.setdefault(k, v)
                        scheduler_config = json.loads(
                            os.path.expandvars(json.dumps(scheduler_config))
                        )
                        targets.append(scheduler_config)
                        create_monitor(
                            scheduler_config,
                            address["ip"],
                            port["port"],
                            target["url"],
                            target["name"],
                        )
        if isinstance(target["url"], str):
            targets.append(target)
    config["targets"] = targets
    log.debug(
        f"expanded config from {pre_target_length} targets to {len(targets)} targets"
    )
    return config


def create_monitor(target, ip, port, original_target_url, original_target_name):
    monitors[target["name"]] = {
        "target": target,
        "ip": ip,
        "port": port,
        "original_target_url": original_target_url,
        "original_target_name": original_target_name,
    }


def validate_config_file(ctx, param, value):
    config = SumoPrometheusScraperConfig()
    try:
        return config.config_schema(json.load(value))
    except JSONDecodeError as e:
        raise click.BadParameter(str(e), ctx=ctx, param=param)
    except MultipleInvalid as e:
        raise click.BadParameter(e.msg, ctx=ctx, param=param, param_hint=e.path)


@click.command()
@click.argument(
    "config",
    envvar="CONFIG_PATH",
    callback=validate_config_file,
    type=click.File("r"),
    default="config.json",
)
def scrape(config):
    expanded_config = expand_config(config)
    scheduler.configure(
        timezone="UTC",
        executors={"default": ThreadPoolExecutor(len(expanded_config["targets"]))},
    )
    for target_config in expanded_config["targets"]:
        scheduler_config = {}
        scheduler_config.update(target_config)
        for k, v in config["global"].items():
            scheduler_config.setdefault(k, v)
        name = target_config["name"]
        scheduler_config = json.loads(os.path.expandvars(json.dumps(scheduler_config)))
        scraper = SumoPrometheusScraper(name, scheduler_config)
        scheduler.add_job(
            func=scraper.run,
            name=name,
            id=name,
            trigger="interval",
            seconds=scheduler_config["run_interval_seconds"],
        )
    scheduler.start()


if __name__ == "__main__":
    scrape()
