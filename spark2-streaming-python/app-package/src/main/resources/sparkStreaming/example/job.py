#
# Name:       Job.py
# Purpose:    Application entry point to create, configure and start spark streaming job.
# Author:     PNDA team
#
# Created:    22/01/2018
#
#
#
# Copyright (c) 2018 Cisco and/or its affiliates.
#
# This software is licensed to you under the terms of the Apache License, Version 2.0 (the "License").
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
#
# The code, technical concepts, and all information contained herein, are the property of Cisco Technology, Inc.
# and/or its affiliated entities, under various laws including copyright, international treaties, patent,
# and/or contract. Any use of the material herein must be in accordance with the terms of the License.
# All rights not expressly granted by the License are reserved.
#
# Unless required by applicable law or agreed to separately in writing, software distributed under the
# License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied.
#
# Attribution:
# The calculation performed is taken from the Spark 2 example "structured streaming event time window example"
# https://github.com/apache/spark/blob/v2.2.1/examples/src/main/python/sql/streaming/structured_network_wordcount_windowed.py
#

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import window

LOG_LEVEL_ERROR = 0
LOG_LEVEL_INFO = 1
LOG_LEVEL_DEBUG = 2
APP_LOG_LEVEL = LOG_LEVEL_INFO

def log_level_name(level):
    if level == LOG_LEVEL_DEBUG:
        return 'DEBUG'
    elif level == LOG_LEVEL_INFO:
        return 'INFO'
    else:
        return 'ERROR'

def log_level_value(level):
    if level == 2:
        return 'DEBUG'
    elif level == 1:
        return 'INFO'
    else:
        return 'ERROR'

def log_out(msg_level, message):
    if APP_LOG_LEVEL >= msg_level:
        print '%s %s %s' % (str(datetime.now()), log_level_name(msg_level), message)

def main():
    global APP_LOG_LEVEL
    log_out(LOG_LEVEL_INFO, 'Loading application.properties')
    properties = dict(line.strip().split('=', 1) if not line.strip().startswith('#') else [line, None]
                    for line in open('application.properties'))
    APP_LOG_LEVEL = log_level_value(['component.log_level'])

    app_name = properties['component.application']
    checkpoint_directory = properties['component.checkpoint_path']
    input_host = properties['component.input_data_host']
    input_port = properties['component.input_data_port']
    window_interval = properties['component.query_window']
    slide_interval = properties['component.query_slide']

    log_out(LOG_LEVEL_INFO, 'Make sure that before running this application you have')
    log_out(LOG_LEVEL_INFO, 'run the following command to create a source of data:')
    log_out(LOG_LEVEL_INFO, '"nc -lk %s" on %s' % (input_port, input_host))

    log_out(LOG_LEVEL_INFO, 'Creating spark context')
    spark = SparkSession.builder.appName(app_name).getOrCreate()

    lines = spark.readStream.format('socket') \
                            .option('host', input_host) \
                            .option('port', input_port) \
                            .option('includeTimestamp', 'true') \
                            .load()

    words = lines.select(
                    explode(split(lines.value, ' ')).alias('word'),
                    lines.timestamp
                  )

    windowed_counts = words.groupBy(window(words.timestamp, window_interval, slide_interval), words.word) \
                           .count() \
                           .orderBy('window')

    query = windowed_counts.writeStream.outputMode('complete') \
                                       .format('console') \
                                       .option('truncate', 'false')

    query.option('checkpointLocation', checkpoint_directory)

    log_out(LOG_LEVEL_INFO, 'Starting spark streaming execution')
    query.start().awaitTermination()

if __name__ == "__main__":
    main()
