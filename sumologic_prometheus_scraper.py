import ast
import gzip
import json
import logging
import os
import requests
import sys
import time

from prometheus_client.parser import text_string_to_metric_families
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

logging.basicConfig(level="INFO", format="%(asctime)s [level=%(levelname)s] [line=%(lineno)d]: %(message)s")
log = logging.getLogger(__name__)


class SumoPrometheusScraper:

    def __init__(self):
        self.config_path = os.environ.get('CONFIG_PATH', './config.json')
        self.target_threads = int(os.environ.get('TARGET_THREADS', 10))
        self.post_threads = int(os.environ.get('POST_THREADS', 10))
        self.batch_size = int(os.environ.get('BATCH_SIZE', 1000))
        self.config = {}

    def run(self):
        self.__validate_config()
        for target in self.config['targets']:
            self.__validate_target(target)
            log.info("getting data for target {0}".format(target))
            metrics = self.__scrape_metrics(target)
            log.info("will send {0} metrics to sumo".format(len(metrics)))
            batches = list(self.__chunk_metrics(metrics_list=metrics))
            headers = self.__get_headers(global_config=self.config.get('global', {}), target_config=target)
            log.info("pushing to sumo with headers: {0}".format(headers))
            for batch in batches:
                log.debug("size before compression: {0} mb".format(sys.getsizeof(batch) / 1024 / 1024))
                compressed_batch = gzip.compress(data=batch.encode(encoding='utf-8'), compresslevel=1)
                log.debug("size  after compression: {0} mb".format(sys.getsizeof(compressed_batch) / 1024 / 1024))
                response = requests.post(url=self.config.get('sumo_http_url'), headers=headers, data=compressed_batch)
                if response.status_code != 200:
                    log.error("error sending batch to sumo: {0}".format(response.content))

    def __validate_config(self):
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

    @staticmethod
    def __validate_target(target):
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
    def __requests_retry_session(retries=5, backoff_factor=0.5, status_forcelist=None, session=None):
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
    SumoAPILogger = SumoPrometheusScraper()
    SumoAPILogger.run()
