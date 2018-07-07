# KSO Test producer #

Test producer in Python to send data to Kafka for the example spark streaming application from Kafka to OpenTSDB

## Example usage ##

Install dependencies with pip:

    pip install -r requirements.txt

Create topic configuration

Add the following topic config to gobblin MR configuration (Refer to the guide for more details on [Gobblin topic configuration](https://github.com/pndaproject/pnda-guide/blob/develop/streamingest/topic-preparation.md#gobblin-topic-configuration)).
```
# ==== Configure topics ====
...
{ \
    "dataset": "avro.events.\*", \
    "pnda.converter.delegate.class": "gobblin.pnda.PNDAAvroConverter", \
    "pnda.family.id": "avro.events", \
    "pnda.avro.source.field": "src", \
    "pnda.avro.timestamp.field": "timestamp", \
    "pnda.avro.schema": '{"namespace": "pnda.entity","type": "record","name": "event","fields": [ {"name": "timestamp", "type": "long"}, {"name": "src", "type": "string"}, {"name": "host_ip", "type": "string"}, {"name": "rawdata", "type": "bytes"}]}' \
  } \
]
```

Send events in kafka:

	python producer.py --brokerlist <broker_ip:port>

Execute in crontab:

	# KSO producer
	* * * * * python /opt/cisco/kso-producer/producer.py --brokerlist=<broker_ip:port>
