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

package io.miguel0afd.lighthouse

import io.codearte.jfairy.Fairy
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

case class Person(id: Int, firstName: String)

case class Event(id: Int, age: Int)

object CommonData {
  
  val TableSize = 100
  val KafkaBroker = "localhost:9092"
  val Topic = "lighthouse"
  
}

object Fusion extends App {

  // Common configuration
  val sparkConf = new SparkConf(true).setMaster("local[4]").setAppName("Lighthouse - Fusion")
  val sc = new SparkContext(sparkConf)

  // Batch
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._

  val fairy = Fairy.create

  val df = sc.parallelize((1 to CommonData.TableSize)
    .map(i => Person(i, fairy.person().firstName))).toDF()

  df.registerTempTable("persons")

  // Streaming
  val WINDOW_LENGTH = new Duration(5 * 1000)
  val ssc = new StreamingContext(sc, WINDOW_LENGTH)

  val zkQuorum = CommonData.KafkaBroker
  val group = "io.miguel0afd.lighthouse"
  val topicMap = Map(CommonData.Topic -> 4)

  val kafkaDS = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap)

  val windowDStream = kafkaDS.map(_._2).window(WINDOW_LENGTH)
  windowDStream.foreachRDD(line => line.toDF().registerTempTable("lines"))

  val fusion = sqlContext.sql("SELECT * FROM persons INNER JOIN lines ON persons.id = lines.id")
  fusion.foreach(println)

  ssc.start()
  ssc.awaitTermination()

}

object Generator extends App {

  import java.util.Properties
  import kafka.javaapi.producer.Producer
  import kafka.producer.KeyedMessage
  import kafka.producer.ProducerConfig
  import kafka.admin.AdminUtils
  import kafka.utils.ZKStringSerializer
  import org.I0Itec.zkclient.ZkClient

  // Create a ZooKeeper client
  val sessionTimeoutMs = 10000
  val connectionTimeoutMs = 10000
  val zkClient = new ZkClient(
    "localhost:2181", sessionTimeoutMs, connectionTimeoutMs, ZKStringSerializer)

  // Create a topic named "myTopic" with 8 partitions and a replication factor of 3
  val topicName = "lighthouse"
  val numPartitions = 4
  val replicationFactor = 1
  val topicConfig = new Properties

  if(!AdminUtils.topicExists(zkClient, topicName)){
    AdminUtils.createTopic(zkClient, topicName, numPartitions, replicationFactor, topicConfig)
  }

  val prop = new Properties()
  prop.put("metadata.broker.list", CommonData.KafkaBroker)
  prop.put("serializer.class","kafka.serializer.StringEncoder")
  val producerConfig = new ProducerConfig(prop)
  val producer = new Producer[String, String](producerConfig)

  val fairy = Fairy.create

  for(i <- 1 to CommonData.TableSize){
    val event = new Event(i, fairy.person().age)
    val message = new KeyedMessage[String, String](
      CommonData.Topic,
      event.id + ", " + event.age)
    println(event)
    producer.send(message)
    Thread.sleep(1000)
  }

  producer.close
}