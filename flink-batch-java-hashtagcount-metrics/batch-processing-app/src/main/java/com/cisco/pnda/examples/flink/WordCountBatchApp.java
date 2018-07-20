/**
  * Name:       WordCountBatchApp
  * Purpose:    Application entry point to create, configure and start flink batch processing job.
  * Author:     PNDA team
  *
  * Created:    30/04/2018
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

package com.cisco.pnda.examples.flink;

import java.util.Calendar;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.concurrent.TimeUnit;

import org.apache.flink.util.Collector;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;

import org.apache.flink.metrics.Counter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.Accumulator;

import com.cisco.pnda.examples.flink.util.WordCountData;
import com.cisco.pnda.examples.flink.*;

import java.io.FileWriter;

final class Tokenizer extends RichFlatMapFunction<String, Tuple2<String, Integer>> {
    private String specific_word;
    private boolean inputFileGiven;
    public static Counter counter;
    private IntCounter accumulator_count= new IntCounter();

    public Tokenizer(String specific_word, boolean inputFileGiven){
        this.specific_word = specific_word;
        this.inputFileGiven = inputFileGiven;
    }

    @Override
    public void open(Configuration config) {
        counter = getRuntimeContext().getMetricGroup().counter("sample-counter");
		getRuntimeContext().addAccumulator("sample-accumulator", this.accumulator_count);
    }

    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
        // String[] tokens = value.toLowerCase().split("\\W+");
        String[] tokens = value.toLowerCase().split("\\s+");

        for (String token : tokens) {

            // Added pause to increase application execution time for default datasets
            if (!inputFileGiven) {
                TimeUnit.SECONDS.sleep(1);
            }

            if (token.length() > 0) {

                try {
                    FileWriter wr = new FileWriter("abc.txt");
                    wr.write(this.specific_word + " " + String.valueOf(token) + " "  + String.valueOf(token.equals(this.specific_word)));
                    wr.close();
                }
                catch (Exception e){
                    e.printStackTrace();
                }
                if (token.equals(this.specific_word)) {
                    counter.inc();
                    this.accumulator_count.add(1);
                    String metricName =  String.format("Current Count (word=%s)", this.specific_word);
                    Tokenizer.doSend(metricName, this.accumulator_count);
                }
                out.collect(new Tuple2<String, Integer>(token, 1));
            }
        }
    }

    public static void doSend(String MetricName, IntCounter MetricValue){
        Properties props = AppConfig.loadProperties();
        String metricsUrl = props.getProperty("environment.metric_logger_url");
        // String metricsUrl = "http://10.0.1.1";
        String appName = props.getProperty("component.application");
        Logger logger = Logger.getLogger(WordCountBatchApp.class.getName());
        String receivedValue = MetricValue.toString();
        String[] receivedValueArr = receivedValue.split(" ");

        try{
            DefaultHttpClient httpClient = new DefaultHttpClient();
            HttpPost post = new HttpPost(metricsUrl);
            post.setHeader("Content-type", "application/json");

            long ts = Calendar.getInstance().getTimeInMillis();
            String body;

            body = String.format("{" +
                            "\"data\": [{ \"source\": \"application.%s\", " +
                            "\"metric\": \"application.kpi.%s.%s\", " +
                            "\"value\":\"%s\", \"timestamp\":\"%s\"}]," +
                            "\"timestamp\": \"%s\"" + "}",
                    appName, appName, MetricName, receivedValueArr[1], ts, ts);

            logger.info(body);
            post.setEntity(new StringEntity(body));
            HttpResponse response = httpClient.execute(post);

            if (response.getStatusLine().getStatusCode() != 200){
                logger.severe("POST failed: " + metricsUrl + " response:" + response.getStatusLine().getStatusCode());
            }
        }
        catch (Exception e) {
            logger.severe("POST failed: " + metricsUrl);
            e.printStackTrace();
        }
    }
}

@SuppressWarnings("serial")
public class WordCountBatchApp{

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);

        // User input to decide default execution or user input based.
        String specificWord = "#test";
        if (params.has("word")) {
            specificWord = params.get("word");
        }

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


        // get input data
        DataSet<String> text;
        boolean inputFileGiven = false;
        if (params.has("input")) {
            // read the text file from given input path
            text = env.readTextFile(params.get("input"));
            inputFileGiven = true;
        }
        else {
            // get default test text data
            System.out.println("Executing WordCount example with default input data set.");
            System.out.println("Use --input to specify file input.");
            text = WordCountData.getDefaultTextLineDataSet(env);
        }

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);
        env.getConfig().setLatencyTrackingInterval(5L);
        Tokenizer token_obj = new Tokenizer(specificWord, inputFileGiven);

        DataSet<Tuple2<String, Integer>> counts =
            // split up the lines in pairs (2-tuples) containing: (word,1)
            text.flatMap(token_obj)
            // group by the tuple field "0" and sum up tuple field "1"
            .groupBy(0)
            .sum(1);

        // emit result
        if (params.has("output")) {
                counts.writeAsCsv(params.get("output"), "\n", " ");
                // execute program
                env.execute("WordCount Example");
        }
        else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            counts.print();
        }

        JobExecutionResult result;
        Integer specific_wordcount;

        // result = env.getLastJobExecutionResult().getAccumulatorResult("which-count");
        specific_wordcount = env.getLastJobExecutionResult().getIntCounterResult("sample-accumulator");
        System.out.println("Accumulator Result for " + specificWord +" Word Count : " + specific_wordcount);
    }
}
