/*
 *
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package io.miguel0afd.lighthouse.launcher

import java.io.File
import java.util.Calendar

import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}

object WriteDate extends App {

  val conf = new SparkConf()
    .setMaster("spark://Miguels-MacBook-Pro.local:7077")
    .setAppName("lighthouse_date")

  val sc = new SparkContext(conf)

  val ct = Calendar.getInstance()

  val array = Array(
    s"App: ${sc.applicationId}",
    s"Year: ${ct.get(Calendar.YEAR)}",
    s"Month: ${ct.get(Calendar.MONTH)}" ,
    s"Day: ${ct.get(Calendar.DAY_OF_MONTH)}" ,
    s"Hour: ${ct.get(Calendar.HOUR_OF_DAY)}",
    s"Minute: ${ct.get(Calendar.MINUTE)}",
    s"Second: ${ct.get(Calendar.SECOND)}",
    s"Millisecond: ${ct.get(Calendar.MILLISECOND)}")

  val rdd = sc.parallelize(array)

  FileUtils.deleteDirectory(new File("current_date"))

  rdd.saveAsTextFile("current_date")

  sc.stop()

}
