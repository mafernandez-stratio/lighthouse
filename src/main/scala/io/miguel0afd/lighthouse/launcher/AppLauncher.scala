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

import java.io.{InputStream, InputStreamReader, BufferedReader}

import org.apache.commons.io.IOUtils
import org.apache.spark.launcher.SparkLauncher

object AppLauncher extends App {

  private def runApp(name: String): Unit = {
    val sparkApp = new SparkLauncher()
      .setSparkHome("/Users/miguelangelfernandezdiaz/workspace/spark-1.5.1-bin-hadoop2.6")
      .setAppResource("/Users/miguelangelfernandezdiaz/workspace/lighthouse/target/lighthouse-0.1-SNAPSHOT.jar")
      .setMainClass("io.miguel0afd.lighthouse.launcher.WriteDate")
      .setMaster("spark://Miguels-MacBook-Pro.local:7077")
      .launch()

    new Thread(new ResultTracker("Input", sparkApp.getInputStream)).start()
    new Thread(new ResultTracker("Error", sparkApp.getErrorStream)).start()

    val result = sparkApp.waitFor()

    println(s"[${name}] Result: " + result)
  }

  runApp("AppLauncher")
}

class ResultTracker(name: String, is: InputStream) extends Runnable {

  val bufReader = new BufferedReader(new InputStreamReader(is))
  val lineIterator = IOUtils.lineIterator(bufReader)

  override def run(): Unit = {
    while (lineIterator.hasNext()) {
      println(s"[${name}]: ${lineIterator.nextLine()}")
    }
  }

}
