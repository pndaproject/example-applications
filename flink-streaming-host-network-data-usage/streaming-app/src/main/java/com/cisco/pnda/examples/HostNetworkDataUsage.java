/**
  * Name:       HostNetworkDataUsage
  * Purpose:    Application entry point to create, configure and start flink streaming job.
  * Author:     PNDA team
  *
  * Created:    24/05/2018
  */

/*
Copyright (c) 2018 Cisco and/or its affiliates.
This software is licensed to you under the terms of the Apache License, Version 2.0 (the "License").
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
The code, technical concepts, and all information contained herein, are the property of Cisco Technology, Inc.
and/or its affiliated entities, under various laws including copyright, international treaties, patent,
and/or contract. Any use of the material herein must be in accordance with the terms of the License.
All rights not expressly granted by the License are reserved.
Unless required by applicable law or agreed to separately in writing, software distributed under the
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
express or implied.
*/

package com.cisco.pnda.examples;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.DecimalFormat;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParseException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.delta.DeltaFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.DeltaTrigger;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

import com.cisco.pnda.examples.util.AvroSchemaDeserialization;
import com.cisco.pnda.examples.util.AvroSchemaSerialization;
import com.cisco.pnda.examples.util.NetworkData;

/**
 * An example of grouped stream windowing where different eviction and trigger
 * policies can be used. The streaming example application triggers the top
 * download/upload speed (bytes/second) of each network interface every x bytes
 * of data download/upload elapsed for the last y seconds.
 */

@SuppressWarnings({ "deprecation", "deprecation" })
public class HostNetworkDataUsage {

	static String topic = "avro.pnda.netmeter";
	static int evictionSec = 10;
	static double triggerBytes = 1024;
	private static String TRIGGER_RECEIVED = "RX";
	private static String TRIGGER_TRANSMITED = "TX";

	static SerializationSchema<HostNwInterface> ser = new AvroSchemaSerialization<HostNwInterface>(
			HostNwInterface.class);
	static DeserializationSchema<HostNwInterface> deser = new AvroSchemaDeserialization<HostNwInterface>(
			HostNwInterface.class);

	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);

		// Read input parameters and configure if available.
		if (params.has("trigger-bytes")) {
			triggerBytes = Double.valueOf(params.get("trigger-bytes"));
		}
		if (params.has("eviction-window")) {
			evictionSec = Integer.valueOf(params.get("eviction-window"));
		}
		if (params.has("topic")) {
			topic = params.get("topic");
		}

		// Read the Zookeeper and Kafka information from application properties.
		Properties prop = new Properties();
		InputStream input = null;
		String kafkaPort = "";
		String zkPort = "";
		try {
			input = new FileInputStream("application.properties");

			// load a properties file
			prop.load(input);

			// get the property value and print it out
			kafkaPort = prop.getProperty("environment.kafka_brokers");
			zkPort = prop.getProperty("environment.kafka_zookeeper");
		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", kafkaPort);
		properties.setProperty("group.id", topic);
		properties.setProperty("zookeeper.connect", zkPort);
		properties.setProperty("batch.size", "0");

		/*
		 * Data-Source
		 *
		 * Avro records are read, de-serialize and mapped to the network data in Tuple7 as
		 * String - host-name
		 * String - Network interface name
		 * Long - Download bytes count ( rx bytes )
		 * Long - Upload bytes count ( tx bytes )
		 * Long - time-stamp ( record generation time )
		 * Double - Download byte rate ( counted using consecutive records )
		 * Double - Upload byte rate ( counted using consecutive records )
		 */


		DataStream<Tuple7<String, String, Long, Long, Long, Double, Double>> networkData = env
				.addSource(new FlinkKafkaConsumer011<HostNwInterface>(topic, deser, properties))
				.map(new ParseNetworkData());

		// Reduce data using consecutive records to get download and upload
		// speed for network interface
		DataStream<Tuple7<String, String, Long, Long, Long, Double, Double>> reducedData = networkData.keyBy(0, 1)
				.reduce(new ReduceFunction<Tuple7<String, String, Long, Long, Long, Double, Double>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple7<String, String, Long, Long, Long, Double, Double> reduce(
							Tuple7<String, String, Long, Long, Long, Double, Double> val1,
							Tuple7<String, String, Long, Long, Long, Double, Double> val2) throws Exception {

						DecimalFormat decimalFormat = new DecimalFormat("#.00");
						val2.f5 = Double
								.valueOf(decimalFormat.format((val2.f2 - val1.f2) / ((val2.f4 - val1.f4) / 1000.00)));
						val2.f6 = Double
								.valueOf(decimalFormat.format((val2.f3 - val1.f3) / ((val2.f4 - val1.f4) / 1000.00)));
						return new Tuple7<>(val2.f0, val2.f1, val2.f2, val2.f3, val2.f4, val2.f5, val2.f6);

					}
				});

		/*
		 * Transformation-1 : Generate triggers for download(rx) bytes.
		 * Transformation-2 : Generate triggers for upload(tx) bytes.
		 * 
		 * Distinct transformation for upload and download triggers because - 
		 * 1. Triggers have delta bytes for different fields (rx and tx bytes). 
		 * 2. Triggers have aggregation on different fields (rx and tx byte rate).
		 */

		// Transformation-1
		@SuppressWarnings("serial")
		DataStream<Tuple8<String, String, String, Long, Long, Long, Double, Double>> RxTriggerData = reducedData
				// Assigns time-stamps to the elements in the data stream and
				// periodically creates water-marks to signal event time
				// progress.
				.assignTimestampsAndWatermarks(new InterfaceTimestamp())
				// Partitions the operator state of a DataStream by the given
				// key positions.
				.keyBy(0, 1)
				// Windows this data stream to a WindowedStream.
				.window(GlobalWindows.create())
				// Sets the Evictor that should be used to evict elements from a
				// window before emission.
				.evictor(TimeEvictor.of(Time.of(evictionSec, TimeUnit.SECONDS)))
				// Sets the Trigger that should be used to trigger window
				// emission.
				.trigger(DeltaTrigger.of(triggerBytes,
						new DeltaFunction<Tuple7<String, String, Long, Long, Long, Double, Double>>() {
							private static final long serialVersionUID = 1L;

							@Override
							public double getDelta(
									Tuple7<String, String, Long, Long, Long, Double, Double> oldDataPoint,
									Tuple7<String, String, Long, Long, Long, Double, Double> newDataPoint) {
								return newDataPoint.f2 - oldDataPoint.f2;
							}
						}, networkData.getType().createSerializer(env.getConfig())))
				// Applies an aggregation that gives the maximum element of
				// every window of the data stream by the given position.
				.maxBy(5)
				// Apply a Map transformation on a DataStream to identify
				// download trigger records.
				.map(new MapTriggerData(TRIGGER_RECEIVED));

		// Transformation-2
		DataStream<Tuple8<String, String, String, Long, Long, Long, Double, Double>> TxTriggerData = reducedData
				.assignTimestampsAndWatermarks(new InterfaceTimestamp()).keyBy(0, 1)
				// .windowAll(GlobalWindows.create())
				.window(GlobalWindows.create()).evictor(TimeEvictor.of(Time.of(evictionSec, TimeUnit.SECONDS)))
				.trigger(DeltaTrigger.of(triggerBytes,
						new DeltaFunction<Tuple7<String, String, Long, Long, Long, Double, Double>>() {
							private static final long serialVersionUID = 1L;

							@Override
							public double getDelta(
									Tuple7<String, String, Long, Long, Long, Double, Double> oldDataPoint,
									Tuple7<String, String, Long, Long, Long, Double, Double> newDataPoint) {
								return newDataPoint.f3 - oldDataPoint.f3;
							}
						}, networkData.getType().createSerializer(env.getConfig())))
				.maxBy(6)
				// Apply a Map transformation on a DataStream to identify
				// download trigger records.
				.map(new MapTriggerData(TRIGGER_TRANSMITED));

		// Data Sink
		if (params.has("output")) {
			RxTriggerData.writeAsText(params.get("output"));
			RxTriggerData.print();

			TxTriggerData.writeAsText(params.get("output"));
			TxTriggerData.print();

		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			RxTriggerData.print();
			TxTriggerData.print();
		}
		env.execute("NetworkDataAnalysisWindowingExample");
	}

	private static class InterfaceTimestamp
			extends AscendingTimestampExtractor<Tuple7<String, String, Long, Long, Long, Double, Double>> {
		private static final long serialVersionUID = 1L;

		@Override
		public long extractAscendingTimestamp(Tuple7<String, String, Long, Long, Long, Double, Double> element) {
			return element.f4;
		}
	}

	private static class ParseNetworkData
			extends RichMapFunction<HostNwInterface, Tuple7<String, String, Long, Long, Long, Double, Double>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple7<String, String, Long, Long, Long, Double, Double> map(HostNwInterface eth)
				throws JsonParseException, JsonMappingException, IOException {

			// Reading raw data and changing it to Network data.
			ObjectMapper mapper = new ObjectMapper();
			NetworkData rawData = mapper.readValue(eth.getRawData(), NetworkData.class);

			Double rxByteRate = Double.valueOf(0);
			Double txByteRate = Double.valueOf(0);
			return new Tuple7<>(rawData.getHostname(), rawData.getNwInterface(), rawData.getRxBytes(),
					rawData.getTxBytes(), rawData.getTimestamp(), rxByteRate, txByteRate);
		}
	}

	private static class MapTriggerData extends
			RichMapFunction<Tuple7<String, String, Long, Long, Long, Double, Double>, Tuple8<String, String, String, Long, Long, Long, Double, Double>> {
		private static final long serialVersionUID = 1L;

		private String triggerType;

		public MapTriggerData(String triggerType) {
			this.triggerType = triggerType;
		}

		@Override
		public Tuple8<String, String, String, Long, Long, Long, Double, Double> map(Tuple7 trgData) {
			String trigger = null;
			if (TRIGGER_RECEIVED.equalsIgnoreCase(this.triggerType)) {
				trigger = "Data-Download-Trigger(RX)";
			} else {
				trigger = "Data-Upload-Trigger(TX)";
			}
			return new Tuple8<>(trigger, (String) trgData.f0, (String) trgData.f1, (Long) trgData.f2, (Long) trgData.f3,
					(Long) trgData.f4, (Double) trgData.f5, (Double) trgData.f6);
		}
	}
}
