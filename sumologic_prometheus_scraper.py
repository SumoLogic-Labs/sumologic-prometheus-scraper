#!/usr/bin/env python

import asyncio
import click
import concurrent.futures
import gzip
import json
import logging
import os
import re
import fnmatch
import requests
import math
import time

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
from itertools import islice
from json.decoder import JSONDecodeError
from prometheus_client.parser import text_string_to_metric_families
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from voluptuous import (
    All,
    IsFile,
    Length,
    MultipleInvalid,
    Optional,
    Range,
    Required,
    Schema,
    Url,
)

logging_level = os.environ.get("LOGGING_LEVEL", "INFO")
logging_format = "%(asctime)s [level=%(levelname)s] [thread=%(threadName)s] [module=%(module)s] [line=%(lineno)d]: %(message)s"
logging.basicConfig(level=logging_level, format=logging_format)
log = logging.getLogger(__name__)


def batches(iterator, batch_size: int):
    """ Yields lists of max batch_size from given iterator"""
    while True:
        batch = list(islice(iterator, batch_size))
        if not batch:
            break
        yield batch


def sanitize_labels(labels: dict):
    """Given prometheus metric sample labels, returns labels dict suitable for Carbon2 format"""
    new_labels = dict()
    for label, value in labels.items():
        if value.strip() == "":
            continue
        nv = value.replace(" ", "_")
        new_labels[label] = nv
    return new_labels


def match_regexp(glob_list: list, default: str):
    """Converts a list of glob matches into a single compiled regexp
    If list is empty, a compilation of default regexp is returned instead"""
    if not glob_list:
        return re.compile(default)
    return re.compile(r"|".join(fnmatch.translate(p) for p in glob_list))


def carbon2(name: str, labels: dict, value: float, scrape_ts: int):
    """Converts given prometheus sample into carbon format"""
    intrinsic_labels = " ".join(f"{k}={v}" for k, v in labels.items())
    if intrinsic_labels == "":
        return f"metric={name}  {value} {scrape_ts}"
    else:
        return f"metric={name} {intrinsic_labels}  {value} {scrape_ts}"


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


class SumoPrometheusScraper:
    def __init__(self, name: str, config: dict):
        self._config = config
        self._name = name
        self._batch_size = config["batch_size"]
        self._sumo_session = None
        self._scrape_session = None
        self._exclude_re = match_regexp(self._config["exclude_metrics"], default=r"")
        self._include_re = match_regexp(self._config["include_metrics"], default=r".*")

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

    def _parsed_samples(self, prometheus_metrics: str):
        for metric_family in text_string_to_metric_families(prometheus_metrics):
            for sample in metric_family.samples:
                name, labels, value = sample
                if math.isnan(value):
                    continue
                if self._exclude_re.match(name) and not self._include_re.match(name):
                    continue
                yield name, sanitize_labels(labels), value

    async def _post_to_sumo(self, resp, scrape_ts: int):
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self._config["target_threads"]
        ) as executor:
            event_loop = asyncio.get_event_loop()
            futures = [
                event_loop.run_in_executor(
                    executor, self._compress_and_send, batch, scrape_ts
                )
                for batch in batches(self._parsed_samples(resp.text), self._batch_size)
            ]
        for _ in await asyncio.gather(*futures):
            pass

    def _compress_and_send(self, batch, scrape_ts: int):
        carbon2_batch = [carbon2(*sample, scrape_ts=scrape_ts) for sample in batch]
        carbon2_batch.append(f"metric=up  1 {scrape_ts}")
        body = "\n".join(carbon2_batch).encode("utf-8")
        resp = self._sumo_session.post(
            self._config["sumo_http_url"],
            data=gzip.compress(body, compresslevel=1),
            headers={
                "Content-Type": "application/vnd.sumologic.carbon2",
                "Content-Encoding": "gzip",
            },
        )
        resp.raise_for_status()
        log.info(
            f"posting batch to Sumo logic for {self._config['name']} took {resp.elapsed.total_seconds()} seconds"
        )

    def run(self):
        start = int(time.time())
        resp = self._scrape_session.get(self._config["url"])
        resp.raise_for_status()
        scrape_ts = int(time.time())
        log.info(
            f"scrape of {self._config['name']} took {resp.elapsed.total_seconds()} seconds"
        )
        event_loop = asyncio.new_event_loop()
        event_loop.run_until_complete(self._post_to_sumo(resp, scrape_ts))

        log.info(
            f"total time taken for {self._config['name']} was {(time.time() - start)} seconds"
        )


global_config_schema = Schema(
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
target_config_schema = global_config_schema.extend(
    {
        Required("url"): Url(),
        Required("name"): str,
        Required("exclude_metrics", default=[]): list([str]),
        Required("include_metrics", default=[]): list([str]),
        "token_file_path": IsFile(),
        # repeat keys from global to remove default values
        "sumo_http_url": Url(),
        "run_interval_seconds": All(int, Range(min=1)),
        "target_threads": All(int, Range(min=1, max=50)),
        "batch_size": All(int, Range(min=1)),
        "retries": All(int, Range(min=1, max=20)),
        "backoff_factor": All(float, Range(min=0)),
    }
)


config_schema = Schema(
    {
        Required("global", default={}): global_config_schema,
        Required("targets"): All(Length(min=1, max=256), [target_config_schema]),
    }
)


def validate_config_file(ctx, param, value):
    try:
        return config_schema(json.load(value))
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
    scheduler = BlockingScheduler(
        timezone="UTC",
        executors={"default": ThreadPoolExecutor(len(config["targets"]))},
    )
    for target_config in config["targets"]:
        scheduler_config = {}
        scheduler_config.update(target_config)
        for k, v in config["global"].items():
            scheduler_config.setdefault(k, v)
        name = target_config["name"]
        if "sumo_http_url" not in scheduler_config:
            log.error(f"sumo_http_url not defined for {name} and is required.")
        else:
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
