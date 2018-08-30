# Example Applications

This repository contains a number of example applications that can be built and run on PNDA. Each application directory contains more detailed information.

## Spark Streaming

Examples of consuming data from Kafka and populating both HBase and OpenTSDB with simple Scala based Spark Streaming applications.

- [Write to HBase](spark-streaming) (scala)
- [Write to OpenTSDB](kafka-spark-opentsdb) (scala)
- [Count messages](spark-streaming-python) (python)

## Spark

Example of consuming data ingested by Gobblin on a batch basis and producing Parquet datasets, optimized for consumption by Impala.
- [Write to parquet format](spark-batch) (scala)
- [Write to parquet format](spark-batch-python) (python)

## Jupyter

[Example of a notebook](jupyter-notebooks) for manipulating network data.

## H2O

[Application that runs the H2O data science platform as an application on PNDA](h2o-launcher).

## Flink Streaming
- [Count Words](flink-streaming-word-count) (scala) Count the words from Socket.
- [Count Words](flink-wordcount-python-app) (python) Count the words from input file.
- [Flink Windows](flink-streaming-host-network-data-usage) (java) host-network-data-usage illustrating Flink windows, triggers and event processing.
- [Count Hashtags](flink-batch-java-hashtagcount-metrics) (java) specific word count from input file illustrating metrics, counters and accumulators.

## Compound Packages

[An example of a package containing multiple application component](literary-word-count-app) types, in this case a Spark app and related Jupyter notebook.
