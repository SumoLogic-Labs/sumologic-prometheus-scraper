# Change Log

All notable changes to this project will be documented in this file. 

# [2.1.0] (2018-07-13)

  * Add ability to pass callback function to SumoPrometheusScraper

# [2.0.0] (2018-07-06)

  * major refactor, which introduces some breaking changes for the better, hence the major version bump
  * use Pipfile for dependencies management and repeatable builds
  * include README.md, CHANGELOG.md, LICENSE and other files in the built image
  * wildcard support in include_metrics / exclude_metrics
  * stricter config validation
  * allow sumo_http_url to be defined in global config or overridden per target

# [1.0.0] (2018-06-05)

  * allow configuration to use environment variables
  * clean up and refactoring

# [0.0.8] (2018-06-05)

  * switch to alpine image
  * ensure job and instance are part of dimensions so they are included with all metrics

# [0.0.7] (2018-06-05)

  * build in scheduling logic and allow targets to specify the interval they should run.
  * reject NaN values

# [0.0.6] (2018-05-30)

  * async calls for targets and sending to sumo, add retry logic for posting to sumo.

# [0.0.5] (2018-05-18)

  * Add support for verify cert and token.

# [0.0.4] (2018-05-18)

  * Add retry logic to all target calls.

# [0.0.3] (2018-05-18)

  * Better error handling on request to target for data.

# [0.0.2] (2018-05-18)

  * Add ability to include metrics explicitly, in addition to current exclusion logic in each target definition.  
  * If metric value is NaN, convert to 0.
  * Add target name, used to generate up metric to show that service is up and healthy.
  * Always send up metric.

# [0.0.1] (2018-05-21)

  * Initial Release