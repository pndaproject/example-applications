/**
  * Name:       Main
  * Purpose:    Create and start a Spark batch job to do a wordcount.
  * Author:     PNDA team
  *
  * Created:    30/09/2016
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

package com.cisco.pnda.examples.wordcount

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import org.apache.log4j.Logger

object Main {

  private[this] val logger = Logger.getLogger(getClass().getName())

  def main(args: Array[String]) = {
    logger.info("Application starting")

    // Create the main Spark context and an SQL context
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val inputPath = args(0)
    val outputPath = args(1)

    val start: Long = System.currentTimeMillis
    val inputRdd = sc.textFile(inputPath + "*.txt")

    val counts = inputRdd.flatMap(line => line.split(" ")).map(word => (word.replaceAll("[^A-Za-z0-9]", ""), 1)).reduceByKey(_ + _).map(tuple => (tuple._2, tuple._1)).sortByKey(false)
    counts.saveAsTextFile(outputPath)
    val end: Long = System.currentTimeMillis

    logger.info("Application done in " + (end - start))
  }
}

