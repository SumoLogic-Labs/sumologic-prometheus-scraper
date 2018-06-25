# sumologic-prometheus-scraper
The Sumo Logic Prometheus Scraper provides a configurable general purpose mechanism to ingest Prometheus formatted metrics into Sumo Logic.

## Support

The code in this repository has been developed in collaboration with the Sumo Logic community and is not supported via standard Sumo Logic Support channels. For any issues or questions please submit an issue directly on GitHub. The maintainers of this project will work directly with the community to answer any questions, address bugs, or review any requests for new features. 

## License
Released under Apache 2.0 License.

## Usage

This script can be run standalone or as a container.  In order to use the script, you need to provide a configuration file that defines the targets that the script should scrape for metrics.  The path to this configuration should be set in an environment variable `CONFIG_PATH`.  Below is an example configuration.

```
{
  "sumo_http_url": "INSERT_SUMO_HTTP_SOURCE_URL_HERE",
  "global": {
    "source_category": "INSERT_SOURCE_CATEGORY",
    "source_host": "INSERT_SOURCE_HOST",
    "source_name": "INSERT_SOURCE_NAME",
    "dimensions": "INSERT_DIMENSIONS_HERE",
    "metadata": "INSERT_METADATA_HERE"
  },
  "targets": [
    {
      "url": "INSERT_PROMETHEUS_SCRAPE_TARGET_HERE",
      "exclude_metrics": ["EXCLUDE_METRIC_1", "EXCLUDE_METRIC_2", ...]
    }
  ]
}
```

### Config Properties

| Key               | Type | Description                                               | Required  | Default |
| ---               | -----| -----------                                               | --------  | ------- |
| `sumo_http_url`   | {}   | This is the Sumo Logic HTTP URL to send the data to.      | Yes       | None    | 
| `global`          | {}   | This is the global settings that apply to all targets.    | No        | None    |
| `targets`         | []   | A list of targets to scrape and sent to Sumo Logic        | No        | None    |

### Global Properties
| Key                      | Type   | Description                                                                                  | Required  | Default |
| ---                      | -----  | -----------                                                                                  | --------  | ------- |
| `run_interval_seconds`   | int    | The interval in seconds in which the target should be scraped.                               | No        | 60      | 
| `source_category`        | String | The source category to assign to all data from every target, unless overridden in target.    | No        | None    | 
| `source_host`            | String | The source host to assign to all data from every target, unless overridden in target.        | No        | None    | 
| `source_name`            | String | The source name to assign to all data from every target, unless overridden in target.        | No        | None    | 
| `dimensions`             | String | Additional dimensions to assign to all data from every target, unless overridden in target.  | No        | None    | 
| `metadata`               | String | Additional metadata to assign to all data from every target, unless overridden in target.    | No        | None    | 

### Target Properties
| Key                       | Type      | Description                                                                                                                                           | Required  | Default | Overrides Global |
| ---                       | -----     | -----------                                                                                                                                           | --------  | ------- | ---------------- |
| `url`                     | String    | The URL for the Prometheus target to scrape.                                                                                                          | Yes       | None    | N/A              |
| `name`                    | String    | The name of the target.  Used to generate an `up` metric to show that target is up.                                                                   | Yes       | None    | N/A              |
| `exclude_metrics`         | \[String\]| A list of Strings of metric names to exclude.  Metrics with this name will not be sent to Sumo Logic.                                                 | No        | None    | N/A              |
| `include_metrics`         | \[String\]| A list of Strings of metric names to include.  Metrics with this name will not be sent to Sumo Logic, as long as they are not in the exclude list.    | No        | None    | N/A              |
| `source_category`         | String    | The source category to assign to all data from every target.  Takes precedence over global setting.                                                   | No        | None    | Yes              |
| `source_host`             | String    | The source host to assign to all data from every target.  Takes precedence over global setting.                                                       | No        | None    | Yes              | 
| `source_name`             | String    | The source name to assign to all data from every target.  Takes precedence over global setting.                                                       | No        | None    | Yes              | 
| `dimensions`              | String    | Additional dimensions to assign to all data from every target.  Takes precedence over global setting.                                                 | No        | None    | Yes              | 
| `metadata`                | String    | Additional metadata to assign to all data from every target.  Takes precedence over global setting.                                                   | No        | None    | Yes              |
| `run_interval_seconds`    | int       | The interval in seconds in which the target should be scraped.  Takes precedence over global setting.                                                 | No        | None    | Yes              |
 
 
### Including and Excluding metrics

For each target, you can provide a list of metrics to include or exclude.  If you are using include and exclude, then exclusion takes precedence.  If you are using include then only metrics in the inclusion list will be sent to Sumo Logic, provided there is no exclusion list containing that same value.

### Setup

#### Create a hosted collector and HTTP source in Sumo

In this step you create, on the Sumo service, an HTTP endpoint to receive your logs. This process involves creating an HTTP source on a hosted collector in Sumo. In Sumo, collectors use sources to receive data.

1. If you don’t already have a Sumo account, you can create one by clicking the **Free Trial** button on https://www.sumologic.com/.
2. Create a hosted collector, following the instructions on [Configure a Hosted Collector](https://help.sumologic.com/Send-Data/Hosted-Collectors/Configure-a-Hosted-Collector) in Sumo help. (If you already have a Sumo hosted collector that you want to use, skip this step.)  
3. Create an HTTP source on the collector you created in the previous step. For instructions, see [HTTP Logs and Metrics Source](https://help.sumologic.com/Send-Data/Sources/02Sources-for-Hosted-Collectors/HTTP-Source) in Sumo help. 
4. When you have configured the HTTP source, Sumo will display the URL of the HTTP endpoint. Make a note of the URL. You will use it when you configure the script to send data to Sumo. 

#### Deploy the script as you want to
The script can be configured with the following environment variables to be set.

| Variable            | Description                                                  | Required | DEFAULT VALUE    |
| --------            | -----------                                                  | -------- | -------------    |
| `CONFIG_PATH`       | The path to the configuration file.                          | YES      |  `./config.json` |
| `TARGET_THREADS`    | The number of threads to use to process the targets.         | NO       |  `10`            | 
| `POST_THREADS`      | The number of threads to use to when posting to Sumo Logic.  | NO       |  `10`            |
| `BATCH_SIZE`        | The number of metrics per batch when posting to Sumo Logic.  | NO       |  `1000`          |
| `LOGGING_LEVEL`     | The logging level.                                           | NO       |  `INFO`          |

##### Running locally

  1. Clone this repo.
  2. Create the configuration file.  If config file is not in the same path as script, set CONFIG_PATH environment variable to config file path.
  3. Install [pipenv](https://docs.pipenv.org/#install-pipenv-today)
  4. Create local virtualenv with all required dependencies `pipenv install`
  5. Activate created virtualenv by running `pipenv shell`
  6. Run the script. `./sumologic_prometheus_scraper.py`
  
##### Running as a Docker Container

The script is packaged as a Docker Container, however the config file is still required and no default is provided.

##### Updating python dependencies

This project uses `Pipfile` and `Pipfile.lock` files to manage python dependencies and provide repeatable builds. 
To update packages you should run `pipenv update` or follow [pipenv upgrade workflow](https://docs.pipenv.org/basics/#example-pipenv-upgrade-workflow)

### Common Errors

#### `No Config Path was defined.`
You did not provide the config path or set the CONFIG_PATH variable.

#### `Config Path was defined but does not exist.`
The config path is defined, but the file does not exist.  Make sure the config path is correct and the file does exist.

#### `Config file is not value JSON.`
The config file has invalid syntax and cannot be parsed as JSON.

#### `Config is empty.`
The configuration file is present and proper JSON, but is empty.

#### `No targets specified.`
There are no targets defined in the config file.

#### `Sumo HTTP Source URL not defined.`
The `sumo_http_url` is not defined.

#### `Sumo HTTP Source URL is empty.`
The `sumo_http_url` is defined with empty value.

#### `Target config url is not defined:`
The target does not have the `url` property defined.