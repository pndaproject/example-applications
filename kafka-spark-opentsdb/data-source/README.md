# KSO Test producer #

Test producer in Python to send data to Kafka for the example spark streaming application from Kafka to OpenTSDB

## Example usage ##

Install dependencies with pip:

    pip install -r requirements.txt

Send events in kafka:

	python producer.py --brokerlist 127.0.0.1:9092

Execute in crontab:

	# KSO producer
	* * * * * python /opt/cisco/kso-producer/producer.py --brokerlist=10.10.10.115:9092
