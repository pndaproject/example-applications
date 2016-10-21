/**
  * Name:       Main
  * Purpose:    Create and start a Spark batch job to convert avro data to parquet.
  * Author:     PNDA team
  *
  * Created:    07/04/2016
  */

/*
Copyright (c) 2016 Cisco and/or its affiliates.

This software is licensed to you under the terms of the Apache License, Version 2.0 (the "License").
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

The code, technical concepts, and all information contained herein, are the property of Cisco Technology, Inc.
and/or its affiliated entities, under various laws including copyright, international treaties, patent,
and/or contract. Any use of the material herein must be in accordance with the terms of the License.
All rights not expressly granted by the License are reserved.

Unless required by applicable law or agreed to separately in writing, software distributed under the
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
either express or implied.
*/

package com.cisco.pnda.examples.batch

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.{AvroInputFormat, AvroWrapper}
import org.apache.hadoop.io.NullWritable

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType,StructField,StringType}

import org.apache.log4j.Logger

object Main {

  private[this] val logger = Logger.getLogger(getClass().getName())

  def main(args: Array[String]) = {
    logger.info("Application starting")

    // Create the main Spark context and an SQL context
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // Load the input data, at the path given by the first arg
    val path = args(0)
    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive","true")
    val avroRDD = sc.hadoopFile[AvroWrapper[GenericRecord], NullWritable, AvroInputFormat[GenericRecord]](path)
    logger.info(avroRDD.count)

    // Extract the rawdata bytes field as a string, and reduce the number of partitions to 10, otherwise if there
    // are a lot of input files there will be too many partitions in the output data.
    val partitions = 10 // This should be exposed in properties.json as a configurable property.
    val payloads = avroRDD.map(x => new String(x._1.datum().get("rawdata").asInstanceOf[java.nio.ByteBuffer].array(),"UTF-8")).coalesce(partitions)

    // Apply a schema to the rawdata field, the fields are:
    // a, b = fixed
    // c = incrementing number
    // gen_ts = timestamp at which the event was generated
    val schemaString = "a b c gen_ts"
    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    val rowRDD = payloads.map(_.split(";")).map(p => Row(p(0), p(1), p(2), p(3)))

    // Save the dataframe as a parquet file, now column based operations on these fields will be efficient in impala
    val df = sqlContext.createDataFrame(rowRDD, schema)
    df.saveAsParquetFile(args(1))

    // Example impala schema:
    // CREATE EXTERNAL TABLE example LIKE PARQUET '/output/path/part-r-00000-64903c65-0d19-4a62-acc0-f38fa929e40a.gz.parquet'
    // STORED AS PARQUET
    // LOCATION '/output/path/';

    logger.info("Application done")
  }
}

