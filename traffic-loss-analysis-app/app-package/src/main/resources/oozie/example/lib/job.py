#
# Name:       job.py
# Purpose:    Detect packet loss using network telemetry data
# Author:     PNDA team
#
# Created:    19/04/2017
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
# License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied.
#

print "starting..."
import sys, json, requests
sys.path.insert(0, '/opt/pnda/app-packages/lib/python2.7/site-packages')
import numpy as np
from pyspark.sql.window import Window
from pyspark.sql import functions as F 
from pyspark.sql.types import LongType, IntegerType, ArrayType, DoubleType, StructType, StructField
from scipy.signal import butter, lfilter, freqz, filtfilt
from pyspark.sql.functions import udf
from pyspark import SparkContext
from pyspark.sql import SQLContext
from platformlibs.simple_data_handler import SimpleDataHandler


# Create the main Spark context and an SQL context
sc = SparkContext(appName="python-spark-tla")
sqlContext = SQLContext(sc)
print "spark version: " + sc.version

metrics = dict([('tla.pkt.tl', 'traffic-loss'), ('tla.pkt.filter', 'filter'), ('tla.pkt.loss', 'pkt-loss'),
                  ('tla.pkt.delta.received', 'delta-packet-received'), ('tla.pkt.delta.sent', 'delta-packet-sent')])
headers = {'content-type': 'application/json'}

def butter_lowpass(cutoff, fs, order=5):
    """ butter low pass """
    nyq = 0.5 * fs
    normal_cutoff = cutoff / nyq
    b, a = butter(order, normal_cutoff, btype='low', analog=False)
    return b, a

def butter_fwbw_lowpass_filter(data, cutoff, fs, order=5):
    """ butter low pass filter """
    b, a = butter_lowpass(cutoff, fs, order=order)
    y = filtfilt(b, a, data, padtype=None)
    return y.tolist()

def save_to_opentsdb(row):
    for key, value in metrics.iteritems():
        data = {
            "metric": key, 
            "timestamp": row['timestamp'],
            "value": row[value],
            "tags": {
                "host": row["host"]
            }
        }
        requests.post(opentsdb_url, data=json.dumps(data), headers=headers)

# zip udf
zip_ = udf(lambda a, b, c, d, e, f, g, h: list(zip(a, b, c, d, e, f, g, h)),
           ArrayType(StructType([StructField('timestamp', LongType()),
                                 StructField('packets-received', LongType()),
                                 StructField('packets-sent', LongType()),
                                 StructField('delta-packet-received', LongType()),
                                 StructField('delta-packet-sent', LongType()),
                                 StructField('pkt-loss-raw', LongType()),
                                 StructField('pkt-loss', DoubleType()),
                                 StructField('filter', DoubleType())])))

# low pass filter udf
butter_fwbw_lowpass_filter_udf = udf(butter_fwbw_lowpass_filter, ArrayType(DoubleType()))

print 'Application starting'

# Load deployment-time parameters
input_data_path = sys.argv[1]
input_data_src = sys.argv[2]
tla_order = int(sys.argv[3])
tla_sample_rate = int(sys.argv[4])
tla_cutoff = int(sys.argv[5])
tla_win_size	= int(sys.argv[6])
tla_threshold	= int(sys.argv[7])
opentsdb_url = sys.argv[8]
logging_level = sys.argv[9]
spark_version = sys.argv[10]

print input_data_path
print input_data_src
print tla_order
print tla_sample_rate
print tla_cutoff
print tla_win_size
print tla_threshold
print opentsdb_url
print logging_level
print spark_version


# Load the input data, at the path given by the first arg
handler = SimpleDataHandler(sc, input_data_src, input_data_path)
rdd = handler.rdd

# Extract raw data from avro data
payloads = rdd.map(lambda x: x['rawdata']) \
                  .map(lambda x: json.loads(x.decode("utf-8"))) \
                  .filter(lambda x: x['metric'] in ["interface.packets-sent", "interface.packets-received"])

df = sqlContext.createDataFrame(payloads) \
                   .select("timestamp", "metric", "value", "tags.host", "tags.interface-name") \
                   .filter(F.col('interface-name').like("%GigabitEthernet%")) \
                   .withColumn('timestamp', F.col('timestamp').cast(LongType())) \
                   .withColumn('packets-sent', F.when(F.col('metric')=='interface.packets-sent', F.col('value').cast(LongType())).otherwise(0)) \
                   .withColumn('packets-received', F.when(F.col('metric')=='interface.packets-received', F.col('value').cast(LongType())).otherwise(0)) \
                   .groupBy('timestamp', 'host', 'interface-name') \
                   .agg(F.sum('packets-sent').alias('packets-sent'), F.sum('packets-received').alias('packets-received'))

host_inf_stats = df.select('host', 'interface-name') \
                  .distinct() \
                  .groupBy('host') \
                  .count() \
                  .select('host', F.col('count').alias('interface-count').cast(IntegerType()))

host_report_stats = df.groupBy('timestamp', 'host') \
               .agg(F.count(F.lit(1)).cast(IntegerType()).alias("num_reports"))

df = df.join(F.broadcast(host_inf_stats), ['host']) \
        .join(F.broadcast(host_report_stats), ['timestamp', 'host']) \
        .filter(F.col('interface-count') == F.col('num_reports')) \
        .groupBy("host", "timestamp") \
        .agg(F.sum('packets-received').alias('packets-received'), F.sum('packets-sent').alias('packets-sent'))

wSpec1 = Window.partitionBy('host').orderBy('timestamp')
df = df.withColumn("pre_recv_val", F.lag(F.col('packets-received')).over(wSpec1)) \
         .withColumn("pre_sent_val", F.lag(F.col('packets-sent')).over(wSpec1)) \
         .withColumn("delta-pkt-received", F.when(F.isnull(F.col('packets-received') - F.col('pre_recv_val')), 0).otherwise(F.col('packets-received')-F.col('pre_recv_val'))) \
         .withColumn("delta-pkt-sent", F.when(F.isnull(F.col('packets-sent') - F.col('pre_sent_val')), 0).otherwise(F.col('packets-sent')-F.col('pre_sent_val'))) \
         .where((F.col("delta-pkt-sent")!=0) & (F.col("delta-pkt-received")!=0)) \
         .withColumn("pkt-loss-raw", F.col("delta-pkt-received")-F.col("delta-pkt-sent")) \
         .select("timestamp", "host", "packets-received", "packets-sent", "delta-pkt-received", "delta-pkt-sent", "pkt-loss-raw")

df = df.withColumn("pkt-loss-raw", F.when(F.abs(F.col('pkt-loss-raw'))<tla_threshold, 0).otherwise(F.col('pkt-loss-raw')))
# normalise
stats = df.select([F.mean('pkt-loss-raw').alias('mean'), F.min('pkt-loss-raw').alias('min'), F.max('pkt-loss-raw').alias('max')]).collect()[0]
wSpec2 = Window.partitionBy('host').orderBy('timestamp').rowsBetween(-(tla_win_size/2),(tla_win_size/2)-1)
df = df.withColumn('pkt-loss', (df['pkt-loss-raw']-stats.mean)/(stats.max-stats.min)) \
         .groupBy('host') \
         .agg(F.collect_list('timestamp').alias('ts-lst'), 
                    F.collect_list('packets-received').alias('packets-received-lst'),
                    F.collect_list('packets-sent').alias('packets-sent-lst'),
                    F.collect_list('delta-pkt-received').alias('delta-pkt-rec-lst'),
                    F.collect_list('delta-pkt-sent').alias('delta-pkt-sent-lst'),
                    F.collect_list('pkt-loss-raw').alias('pkt-loss-raw-lst'),
                    F.collect_list('pkt-loss').alias('pkt-loss-lst')) \
        .withColumn('filter-lst', 
                    butter_fwbw_lowpass_filter_udf(F.col('pkt-loss-lst'), 
                                                   F.lit(tla_cutoff), 
                                                   F.lit(tla_sample_rate), 
                                                   F.lit(tla_order))) \
        .withColumn("tmp", zip_('ts-lst', 
                                'packets-received-lst', 
                                'packets-sent-lst', 
                                'delta-pkt-rec-lst', 
                                'delta-pkt-sent-lst', 
                                'pkt-loss-raw-lst', 
                                'pkt-loss-lst', 
                                'filter-lst')) \
        .withColumn("tmp", F.explode("tmp")) \
        .select('host', 
                F.col("tmp.timestamp"),
                F.col('tmp.packets-received'),
                F.col('tmp.packets-sent'),
                F.col('tmp.delta-packet-received'),
                F.col('tmp.delta-packet-sent'),
                F.col('tmp.pkt-loss-raw'),
                F.col('tmp.pkt-loss'), 
                F.col('tmp.filter')) \
        .withColumn("mean", F.mean('filter').over(wSpec2)) \
        .withColumn("stdev", F.stddev('filter').over(wSpec2)) \
        .withColumn("traffic-loss", F.when(((F.col('mean')>0) & (F.abs(F.col('mean')) > (F.lit(tla_threshold) * F.col('stdev')))), 1).otherwise(0))

# Debug message
print df.count()

# Save results to opentsdb
df.rdd.map(lambda x: save_to_opentsdb(x)).count()

sc.stop()
print 'Application done'