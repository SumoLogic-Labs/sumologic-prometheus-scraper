import ast
import gzip
import json
import logging
import os
import requests
import sys
import time

from prometheus_client.parser import text_string_to_metric_families

logging.basicConfig(level="INFO", format="%(asctime)s [level=%(levelname)s] [line=%(lineno)d]: %(message)s")
log = logging.getLogger(__name__)


class SumoPrometheusScraper:

    def __init__(self):
        self.config_path = os.environ.get('CONFIG_PATH', './config.json')
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

    def __scrape_metrics(self, target):
        prometheus_metrics = requests.get(target['url']).content.decode('utf-8').split('\n')
        scrape_time = int(time.time())
        return self.__format_prometheus_to_carbon2(prometheus_metrics=prometheus_metrics, scrape_time=scrape_time,
                                                   target_config=target)

    @staticmethod
    def __format_prometheus_to_carbon2(prometheus_metrics, scrape_time, target_config):
        metrics = []
        for metric_string in prometheus_metrics:
            for family in text_string_to_metric_families(metric_string):
                for sample in family.samples:
                    metric_data = "{0}::{1}::{2}".format(*sample).split("::")
                    if metric_data[0] not in target_config.get('exclude_metrics', []):
                        carbon2_metric = "metric={0} ".format(metric_data[0])
                        for attr, value in ast.literal_eval(metric_data[1]).items():
                            if value == "":  # carbon2 format cannot accept empty values
                                value = "none"
                            if " " in value:  # carbon2 format cannot accept values with spaces
                                value = value.replace(" ", "_")
                            carbon2_metric += "{0}={1} ".format(attr, value)
                        carbon2_metric += " {0} {1}".format(metric_data[2], scrape_time)
                        metrics.append(carbon2_metric)
        return metrics

    @staticmethod
    def __chunk_metrics(metrics_list, batch_size=5000):
        for i in range(0, len(metrics_list), batch_size):
            yield "\n".join(metrics_list[i:i + batch_size])

    @staticmethod
    def __get_headers(global_config, target_config):
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


if __name__ == '__main__':
    SumoAPILogger = SumoPrometheusScraper()
    SumoAPILogger.run()
