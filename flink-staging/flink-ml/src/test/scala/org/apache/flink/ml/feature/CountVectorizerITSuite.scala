/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.ml.feature

import org.apache.flink.api.scala._
import org.apache.flink.test.util.FlinkTestBase
import org.scalatest.{Matchers, FlatSpec}

class CountVectorizerITSuite
  extends FlatSpec
  with Matchers
  with FlinkTestBase {

  behavior of "The CountVectorizer"

  it should "vectorize text" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val trainingData = Seq(
      "Das ist ein Test und das soll zeigen ob es funktioniert.",
      "Das Kind ist groß und soll ein Test sein.",
      "Funktioniert das hier funktioniert gut?")


    val trainingDataDS = env.fromCollection(trainingData)

    val cv = CountVectorizer()

    cv.fit(trainingDataDS)

    val result =  cv.transform(trainingDataDS).collect()

    println(result.mkString("\n"))
  }
}
