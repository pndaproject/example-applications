"""
Name:       producer.py
Purpose:    Generates test data for kafka-spark-opentsdb example app
            the various component services such as HDFS, HBase etc

Author:     PNDA team

Created:    14/03/2016
"""
import io
import os
import sys
import getopt
import random
import time
from time import gmtime, strftime

import avro.schema
import avro.io

from kafka import KafkaProducer

KAFKA_BROKERLIST = "localhost:9092"

# Path to user.avsc avro schema
HERE = os.path.abspath(os.path.dirname(__file__))
SCHEMA_PATH = HERE + "/dataplatform-raw.avsc"

# Kafka topic
TOPIC = "avro.kso.metrics"
CURRENT_TIME_MILLIS = lambda: int(round(time.time() * 1000))

def run(brokers):
    '''
    Run the test
    '''
    schema = avro.schema.parse(open(SCHEMA_PATH).read())
    producer = KafkaProducer(bootstrap_servers=[brokers])
    writer = avro.io.DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    extrabytes = bytes('')

    for i in range(0, 9):
        collectd_value = random.randint(0, 100)
        collectd_alea = "{'host':'pnda" + str(i) + "','collectd_type':'cpu','value':'" + \
                    str(collectd_value) + "','timestamp':'" + \
                    strftime("%Y-%m-%dT%H:%M:%S.000Z", gmtime()) + "'}"
        writer.write({"timestamp": CURRENT_TIME_MILLIS(), "src": "collectd",
                      "host_ip": "bb80:0:1:2:a00:bbff:bbee:b123",
                      "rawdata": collectd_alea}, encoder)
        raw_bytes = bytes_writer.getvalue()
        # reset buffer to start index
        bytes_writer.seek(0)
        producer.send(TOPIC, extrabytes + raw_bytes)
        time.sleep(1)

        collectd_value = random.randint(0, 4000000)
        collectd_alea = "{'host':'pnda" + str(i) + "','collectd_type':'memory','value':'" + \
            str(collectd_value) + "','timestamp':'" + \
            strftime("%Y-%m-%dT%H:%M:%S.000Z", gmtime()) + "'}"
        writer.write({"timestamp": CURRENT_TIME_MILLIS(),
                      "src": "collectd",
                      "host_ip": "bb80:0:1:2:a00:bbff:bbee:b123",
                      "rawdata": collectd_alea}, encoder)
        raw_bytes = bytes_writer.getvalue()
        # reset buffer to start index
        bytes_writer.seek(0)            
        producer.send(TOPIC, extrabytes + raw_bytes)

        time.sleep(1)


if __name__ == '__main__':

    broker_list = None

    try:
        opts, args = getopt.getopt(sys.argv[1:], "hb:", ["brokerlist="])
    except getopt.GetoptError:
        print 'producer.py [-b localhost:9092] '
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print 'producer.py [--brokerlist broker:port [,broker:port]]'
            sys.exit()
        elif opt in ("-b", "--brokerlist"):
            print "brokerlist: %s" % arg
            broker_list = arg

    run(broker_list if broker_list is not None else KAFKA_BROKERLIST)

