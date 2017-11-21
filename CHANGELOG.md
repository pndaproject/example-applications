# Change Log
All notable changes to this project will be documented in this file.

## Unreleased
### Changed
- PNDA-3401: Change the spark-batch(-python) to output in the user's directory.

### Added
- PNDA-2445: Support for Hortonworks HDP hadoop distro.

## [0.3.0] 2017-05-23
### Changed
 - PNDA-2700: Update spark streaming example to work on redhat.

### Fixed
 - PNDA-3051: Fix timestamp generation for opentsdb datapoints

### Added
- PNDA-2726: Added example spark-batch and spark-streaming jobs in python

## [0.2.0] 2016-12-12
### Added
- PNDA-2359 Move applications to CDH 5.9 which include spark streaming 1.6
- PNDA-2503 Remove explicit memory/vcore settings in apps

### Fixed
- Change Kafka version to 0.10.0.1
- Pin assembly plugin to version 2.6
- Update assembly.xml file to add the id xml tag

## [0.1.0] 2016-10-21
### Added
- h2o-launcher application to run h2o data science platform
- literary-word-count-app to run a classic wordcount
- Spark streaming example app that consumes from Kafka and writes to HBase
- Spark batch example app that consumes Gobblin produced Avro datasets from HDFS and produces Parquet for use with Impala
- Spark streaming example app that consumes from Kafka and writes to OpenTSDB
- Jupyter notebook example that shows some simple network data manipulation


