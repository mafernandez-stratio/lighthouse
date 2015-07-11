/*
 *
 *  * Licensed to STRATIO (C) under one or more contributor license agreements.
 *  * See the NOTICE file distributed with this work for additional information
 *  * regarding copyright ownership.  The STRATIO (C) licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 *
 */

package com.stratio.crossdata.fuse

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.{TaskContext, Partition, Dependency, SparkContext}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class CrossdataRDD[R: ClassTag](
  sc: SparkContext,
  dep: Seq[Dependency[_]])
  extends RDD[R](sc, dep) {

  override def collect(): Array[R] = {
    val fakeResult: Array[R] = new Array[R](0)
    fakeResult
  }

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[R] = ???

  override protected def getPartitions: Array[Partition] = ???
}