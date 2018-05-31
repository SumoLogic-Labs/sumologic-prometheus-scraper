import ast
import asyncio
import datetime
import gzip
import json
import logging
import os
import requests
import sys
import time

import concurrent.futures

from prometheus_client.parser import text_string_to_metric_families
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

logging_level = os.environ.get("LOGGING_LEVEL", "INFO")
logging.basicConfig(level=logging_level,
                    format="%(asctime)s [level=%(levelname)s] [thread=%(threadName)s] [line=%(lineno)d]: %(message)s")
log = logging.getLogger(__name__)


class SumoPrometheusScraper:

    def __init__(self):
        self.config_path = os.environ.get('CONFIG_PATH', './config.json')
        self.target_threads = int(os.environ.get('TARGET_THREADS', 10))
        self.post_threads = int(os.environ.get('POST_THREADS', 10))
        self.batch_size = int(os.environ.get('BATCH_SIZE', 1000))
        self.config = {}

    async def run(self):
        request_start = datetime.datetime.now()
        self._validate_config()
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.target_threads) as executor:
            event_loop = asyncio.get_event_loop()
            futures = [
                event_loop.run_in_executor(
                    executor,
                    self._process_target,
                    target
                )
                for target in self.config['targets']
            ]
        for _ in await asyncio.gather(*futures):
            pass
        log.info("total time taken: {0}".format(datetime.datetime.now() - request_start))

    def scrape_metrics(self, target):
        scrape_time = int(time.time())
        metrics = []
        try:
            headers = {}
            if 'token_file_path' in target:
                with open(target['token_file_path'], 'r') as token_file:
                    headers['Authorization'] = "Bearer {0}".format(token_file.read())
            response = self._requests_retry_session().get(url=target['url'],
                                                          verify=target.get('verify', None),
                                                          headers=headers)
            if response.status_code != 200:
                log.error("received status code {0} from target {1}: {2}".format(response.status_code, target['name'],
                                                                                 response.content))
                raise Exception
            prometheus_metrics = response.content.decode('utf-8').split('\n')
            metrics = self._format_prometheus_to_carbon2(prometheus_metrics=prometheus_metrics,
                                                         scrape_time=scrape_time,
                                                         target_config=target)
            metrics.append("metric=up instance={0} job={1}  1 {2}".format(target['url'], target['name'], scrape_time))
        except Exception as e:
            log.error("unable to scrape metrics from target {0}: {1}".format(target['name'], e))
            metrics.append("metric=up instance={0} job={1}  0 {2}".format(target['url'], target['name'], scrape_time))
        return metrics

    @staticmethod
    def chunk_metrics(metrics_list, batch_size):
        for i in range(0, len(metrics_list), batch_size):
            yield "\n".join(metrics_list[i:i + batch_size])

    async def _post_to_sumo(self, batches, headers, target_name):
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.post_threads) as executor:
            event_loop = asyncio.get_event_loop()
            futures = [
                event_loop.run_in_executor(
                    executor,
                    self._compress_and_send,
                    idx,
                    batch,
                    headers,
                    target_name
                )
                for idx, batch in enumerate(batches)
            ]
        for _ in await asyncio.gather(*futures):
            pass

    @staticmethod
    def _format_prometheus_to_carbon2(prometheus_metrics, scrape_time, target_config):
        metrics = []
        for metric_string in prometheus_metrics:
            for family in text_string_to_metric_families(metric_string):
                for sample in family.samples:
                    metric_data = "{0}::{1}::{2}".format(*sample).split("::")
                    carbon2_metric = "metric={0} ".format(metric_data[0])
                    for attr, value in ast.literal_eval(metric_data[1]).items():
                        if value == "":  # carbon2 format cannot accept empty values
                            value = "none"
                        if " " in value:  # carbon2 format cannot accept values with spaces
                            value = value.replace(" ", "_")
                        carbon2_metric += "{0}={1} ".format(attr, value)
                    if metric_data[2].lower() == "nan":  # carbon2 format cannot accept NaN values
                        carbon2_metric += " {0} {1}".format(0, scrape_time)
                    else:
                        carbon2_metric += " {0} {1}".format(metric_data[2], scrape_time)
                    if not len(target_config.get('include_metrics', [])) == 0:
                        if metric_data[0] in target_config['include_metrics']:
                            if metric_data[0] not in target_config.get('exclude_metrics', []):
                                metrics.append(carbon2_metric)
                    else:
                        if metric_data[0] not in target_config.get('exclude_metrics', []):
                            metrics.append(carbon2_metric)
        return metrics

    def _validate_config(self):
        if self.config_path is None:
            log.error("No Config Path was defined.")
            sys.exit(os.EX_CONFIG)
        if not os.path.exists(self.config_path):
            log.error("Config Path was defined but does not exist.")
            sys.exit(os.EX_CONFIG)
        try:
            with open(self.config_path, 'r') as config_file:
                self.config = json.load(config_file)
        except IOError:
            log.error("Config file is not value JSON.")
            sys.exit(os.EX_CONFIG)
        if len(self.config) == 0:
            log.error("Config is empty.")
            sys.exit(os.EX_CONFIG)
        if 'targets' not in self.config or len(self.config['targets']) == 0:
            log.error("No targets specified.")
            sys.exit(os.EX_CONFIG)
        if 'sumo_http_url' not in self.config:
            log.error("Sumo HTTP Source URL not defined.")
            sys.exit(os.EX_CONFIG)
        if not self.config.get('sumo_http_url', None):
            log.error("Sumo HTTP Source URL is empty.")
            sys.exit(os.EX_CONFIG)

    def _process_target(self, target):
        target_start = datetime.datetime.now()
        self._validate_target(target)
        log.info("target={0}  fetching data".format(target['name']))
        metrics = self.scrape_metrics(target)
        log.info("target={0}  will send {1} metrics to sumo".format(target['name'], len(metrics)))
        batches = list(self.chunk_metrics(metrics_list=metrics, batch_size=self.batch_size))
        headers = self._get_headers(global_config=self.config.get('global', {}), target_config=target)
        log.debug("target={0}  pushing to sumo with headers: {1}".format(target['name'], headers))
        post_loop = asyncio.new_event_loop()
        post_loop.run_until_complete(self._post_to_sumo(batches=batches, headers=headers, target_name=target['name']))
        log.info("target={0}  time taken: {1}".format(target['name'], datetime.datetime.now() - target_start))

    @staticmethod
    def _validate_target(target):
        if 'url' not in target or target['url'] is None:
            log.error("Target config url is not defined: {0}".format(target))
            sys.exit(os.EX_CONFIG)
        if 'name' not in target or target['name'] is None:
            log.error("Target config name is not defined: {0}".format(target))
            sys.exit(os.EX_CONFIG)

    @staticmethod
    def _get_headers(global_config, target_config):
        headers = {'Content-Type': 'application/vnd.sumologic.carbon2', 'Content-Encoding': 'gzip'}
        if 'source_category' in global_config:
            headers['X-Sumo-Category'] = global_config['source_category']
        if 'source_category' in target_config:
            headers['X-Sumo-Category'] = target_config['source_category']
        if 'source_name' in global_config:
            headers['X-Sumo-Name'] = global_config['source_name']
        if 'source_name' in target_config:
            headers['X-Sumo-Name'] = target_config['source_name']
        if 'source_host' in global_config:
            headers['X-Sumo-Host'] = global_config['source_host']
        if 'source_host' in target_config:
            headers['X-Sumo-Host'] = target_config['source_host']
        if 'dimensions' in global_config:
            headers['X-Sumo-Dimensions'] = global_config['dimensions']
        if 'dimensions' in target_config:
            headers['X-Sumo-Dimensions'] = target_config['dimensions']
        if 'metadata' in global_config:
            headers['X-Sumo-Metadata'] = global_config['metadata']
        if 'metadata' in target_config:
            headers['X-Sumo-Metadata'] = target_config['metadata']
        return headers

    def _compress_and_send(self, idx, batch, headers, target_name):
        log.debug("target={0} batch={1}  size before compression: {2} mb".format(target_name, idx,
                                                                                 sys.getsizeof(batch) / 1024 / 1024))
        compressed_batch = gzip.compress(data=batch.encode(encoding='utf-8'), compresslevel=1)
        log.debug("target={0} batch={1} size after compression: {2} mb".format(target_name, idx,
                                                                               sys.getsizeof(
                                                                                   compressed_batch) / 1024 / 1024))
        response = self._requests_retry_session().post(url=self.config.get('sumo_http_url'),
                                                       headers=headers,
                                                       data=compressed_batch)
        if response.status_code != 200:
            log.error("target={0} batch={1}  error sending batch to sumo: {2}".format(target_name, idx,
                                                                                      response.content))
        else:
            log.info("target={0} batch={1}  successfully posted batch to sumo".format(target_name, idx))

    @staticmethod
    def _requests_retry_session(retries=5, backoff_factor=0.2, status_forcelist=None, session=None):
        session = session or requests.Session()
        retry = Retry(
            total=retries,
            read=retries,
            connect=retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session


if __name__ == '__main__':
    SumoPrometheusScraper = SumoPrometheusScraper()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(SumoPrometheusScraper.run())
