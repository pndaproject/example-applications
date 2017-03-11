#
# Name:       job.py
# Purpose:    Create and start a Spark batch job to convert avro data to parquet.
# Author:     PNDA team
#
# Created:    10/03/2017
#

#
# Copyright (c) 2017 Cisco and/or its affiliates.
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
# License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied.
#

import sys
from pyspark import SparkContext
from pyspark.sql import SQLContext
from platformlibs.simple_data_handler import SimpleDataHandler

print 'Application starting'

input_data_path = sys.argv[1]
output_data_path = sys.argv[2]

sc = SparkContext()
sqlContext = SQLContext(sc)
handler = SimpleDataHandler(sc, "test-src", input_data_path)
rdd = handler.rdd

print rdd.count()

partitions = 10
payloads = rdd.map(lambda x: x['rawdata']).coalesce(partitions)
rows = payloads.map(lambda x: x.split(';'))
df = sqlContext.createDataFrame(rows, ['a', 'b', 'c', 'gen_ts'])
df.saveAsParquetFile(output_data_path)

# Example impala schema:
# CREATE EXTERNAL TABLE example LIKE PARQUET '/output/path/part-r-00000-64903c65-0d19-4a62-acc0-f38fa929e40a.gz.parquet'
# STORED AS PARQUET
# LOCATION '/output/path/';

print 'Application done'