# Change Log

All notable changes to this project will be documented in this file. 

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