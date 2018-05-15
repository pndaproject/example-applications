# KSO Test producer #

Test producer in Python to send data to Kafka for the example spark streaming application from Kafka to OpenTSDB

## Example usage ##

Install dependencies with pip:

    pip install -r requirements.txt

Ceate topic configuration

Add the following topic config to gobblin MR configuration, which can be found at edge node at `/opt/pnda/gobblin/configs/mr.pull`
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
