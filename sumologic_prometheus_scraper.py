#!/usr/bin/env python

import ast
import asyncio
import click
import gzip
import json
import logging
import os
import requests
import sys
import time

import concurrent.futures

from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime
from json.decoder import JSONDecodeError
from prometheus_client.parser import text_string_to_metric_families
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from voluptuous import Schema, Url, Required, MultipleInvalid, All, Range, Length, IsFile


logging_level = os.environ.get("LOGGING_LEVEL", "INFO")
logging_format = \
    "%(asctime)s [level=%(levelname)s] [thread=%(threadName)s] [module=%(module)s] [line=%(lineno)d]: %(message)s"
logging.basicConfig(level=logging_level, format=logging_format)
log = logging.getLogger(__name__)


class SumoHTTPAdapter(HTTPAdapter):
    CONFIG_TO_HEADER = (
        'source_category', 'X-Sumo-Category', 
        'source_name', 'X-Sumo-Name', 
        'source_host', 'X-Sumo-Host', 
        'metadata', 'X-Sumo-Metadata', 
    )

    def __init__(self, config, **kwds):
        self._config = config
        super().__init__(max_retries=retry, **kwds)

    def add_headers(self, request, **kwds):
        for config_key, header_name in self.CONFIG_TO_HEADER.items():
            if config_key in self.config:
                request.headers[header_name] = self.config[config_key]

        dimensions = f"job={self.name},instance={self.config['url']}"        
        if 'dimensions' in self.config:
            dimensions += f",{self.config['dimmension']}"
        request.headers['X-Sumo-Dimmensions'] = dimmension


class SumoPrometheusScraper:

    def __init__(self, name, batch_size, **config):
        self._config = config
        self._name = config['name']
        self._batch_size = config['batch_size']
        self._sumo_session = None
        self._scrape_session = None

        retries = config['retries']
        self._default_retry = Retry(total=retries, read=retries, connect=retries, backoff_factor=config['backoff_factor'])

        self._sumo_session = requests.Session()
        adapter = SumoHTTPAdapter(config, max_)
        self._sumo_session.mount('http://', adapter)
        self._sumo_session.mount('https://', adapter)

    def run(self):
        start = time.monotonic()
        log.info("target={0}  fetching data".format(self._name))
        metrics = self.scrape_metrics()
        log.info("target={0}  will send {1} metrics to sumo".format(self.name, len(metrics)))
        batches = list(self.chunk_metrics(metrics_list=metrics, batch_size=self.batch_size))
        log.debug("target={0}  pushing to sumo with headers: {1}".format(self.name))
        event_loop = asyncio.new_event_loop()
        event_loop.run_until_complete(self._post_to_sumo(
            batches=batches, headers=headers, target_name=self.name))
        log.info(f"target={0}  time taken: {start - time.monotonic():%.2f}")

    def scrape_metrics(self):
        start = time.monotonic()
        carbon2_metrics = []
        try:
            headers = {}
            if 'token_file_path' in target_config:
                with open(target_config['token_file_path'], 'r') as token_file:
                    headers['Authorization'] = "Bearer {0}".format(token_file.read())
            resp = self._requests_retry_session().get(
                url=target_config['url'], verify=target_config.get('verify', None), headers=headers)
            if resp.status_code != 200:
                log.error("received status code {0} from target {1}: {2}".format(
                    resp.status_code, self.name, resp.content))
                resp.
            prometheus_metrics = resp.content.decode('utf-8').split('\n')
            carbon2_metrics = self._format_prometheus_to_carbon2(
                prometheus_metrics=prometheus_metrics, scrape_time=scrape_time, target_config=target_config)
            carbon2_metrics.append("metric=up  1 {0}".format(scrape_time))
        except Exception as e:
            log.error("unable to scrape metrics from target {0}: {1}".format(self.name, e))
            carbon2_metrics.append("metric=up  0 {0}".format(scrape_time))
        return carbon2_metrics

    @staticmethod
    def _format_prometheus_to_carbon2(prometheus_metrics, scrape_time, target_config):
        metrics = []
        for metric_string in prometheus_metrics:
            for family in text_string_to_metric_families(metric_string):
                for sample in family.samples:
                    metric_data = "{0}::{1}::{2}".format(*sample).split("::")
                    if metric_data[2].lower() == "nan":  # carbon2 format cannot accept NaN values
                        continue
                    carbon2_metric = "metric={0} ".format(metric_data[0])
                    for attr, value in ast.literal_eval(metric_data[1]).items():
                        if value == "":  # carbon2 format cannot accept empty values
                            value = "none"
                        if " " in value:  # carbon2 format cannot accept values with spaces
                            value = value.replace(" ", "_")
                        carbon2_metric += "{0}={1} ".format(attr, value)
                    carbon2_metric += " {0} {1}".format(metric_data[2], scrape_time)
                    if len(target_config.get('include_metrics', [])) != 0:
                        if metric_data[0] in target_config['include_metrics']:
                            if metric_data[0] not in target_config.get('exclude_metrics', []):
                                metrics.append(carbon2_metric)
                    else:
                        if metric_data[0] not in target_config.get('exclude_metrics', []):
                            metrics.append(carbon2_metric)
        return metrics

    @staticmethod
    def chunk_metrics(metrics_list, batch_size):
        for i in range(0, len(metrics_list), batch_size):
            yield "\n".join(metrics_list[i:i + batch_size])

    async def _post_to_sumo(self, batches, headers, target_name):
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.post_threads) as executor:
            event_loop = asyncio.get_event_loop()
            futures = [
                event_loop.run_in_executor(executor, self._compress_and_send, idx, batch, headers, target_name)
                for idx, batch in enumerate(batches)
            ]
        for _ in await asyncio.gather(*futures):
            pass

    def _get_headers(self):


    def _compress_and_send(self, idx, batch, headers, target_name):
        raw_size = sys.getsizeof(batch) / 1024 / 1024
        log.debug("target={0} batch={1}  size before compression: {2} mb".format(target_name, idx, raw_size))
        compressed_batch = gzip.compress(data=batch.encode(encoding='utf-8'), compresslevel=1)
        compressed_size = sys.getsizeof(compressed_batch) / 1024 / 1024
        log.debug("target={0} batch={1} size after compression: {2} mb".format(target_name, idx, compressed_size))
        resp = self._requests_retry_session().post(
            url=self.config.get('sumo_http_url'), headers=headers, data=compressed_batch)
        if resp.status_code != 200:
            log.error("target={0} batch={1}  error sending batch to sumo: {2}".format(
                target_name, idx, resp.content))
        else:
            log.info("target={0} batch={1}  successfully posted batch to sumo".format(target_name, idx))

    @staticmethod
    def _requests_retry_session(retries=5, backoff_factor=0.2, forcelist=None, session=None):





global_config_schema = Schema({
    Required('run_interval_seconds', default=60): All(int, Range(min=1)),
    Required('target_threads', default=10): All(int, Range(min=1, max=50)),
    Required('batch_size', default=1000): All(int, Range(min=1)),
    Required('retries', default=5): All(int, Range(min=1, max=20)),
    Required('backoff_factor', default=0.2): All(float, Range(min=0)),
    'source_category': str,
    'source_host': str,
    'source_name': str,
    'dimensions': str,
    'metadata': str,
    'token_file_path': IsFile(),
})

target_config_schema = global_config_schema.extend({
    Required('url'): Url(),  
    Required('name'): str,
})

config_schema = Schema({
   Required('sumo_http_url'): Url(),
   Required('global', default={}): global_config_schema,
   Required('targets'): All(Length(min=1), [target_config_schema])
})


def validate_config_file(ctx, param, value):
    try:
        return config_schema(json.load(value))
    except JSONDecodeError as e:
        raise click.BadParameter(str(e), ctx=ctx, param=param)
    except MultipleInvalid as e:
        raise click.BadParameter(e.msg, ctx=ctx, param=param, param_hint=e.path)

def run(config):
    SumoPrometheusScraper(config).run()


@click.command()
@click.argument('config', 
    envvar='CONFIG_PATH', callback=validate_config_file, type=click.File('r'), default="config.json")
def scraper(config):
    start = time.monotonic()

    scheduler = BlockingScheduler(timezone='UTC')
    for target_config in config['targets']:
        scheduler_config = {'sumo_http_url': config['sumo_http_url']}
        scheduler_config.update(target_config)
        for k, v in config['global'].items():
            scheduler_config.setdefault(k, v)

        scheduler.add_job(run, 'interval', config, name=scheduler_config['name'], id=scheduler_config['name'], seconds=interval)
    scheduler.start()


if __name__ == '__main__':
    scraper()