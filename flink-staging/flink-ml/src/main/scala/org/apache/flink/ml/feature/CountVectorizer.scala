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
import org.apache.flink.ml.common.{LabeledVector, ParameterMap}
import org.apache.flink.ml.math.SparseVector
import org.apache.flink.ml.pipeline.{TransformOperation, FitOperation, Transformer}
import org.apache.flink.util.Collector

class CountVectorizer extends Transformer[CountVectorizer] {
  var dictionary: Option[DataSet[Map[String, Int]]] = None
}

object CountVectorizer {
  implicit val fitDictionary = new FitOperation[CountVectorizer, String] {
    override def fit(
        instance: CountVectorizer,
        fitParameters: ParameterMap,
        input: DataSet[String])
      : Unit = {
      val result = trainDictionary(input)

      instance.dictionary = Some(result)
    }
  }

  implicit val fitDictionaryLabeledData = new FitOperation[CountVectorizer, (Double, String)] {
    override def fit(instance: CountVectorizer, fitParameters: ParameterMap, input: DataSet[
      (Double, String)]): Unit = {
      val strippedInput = input.map(x => x._2)

      instance.dictionary = Some(trainDictionary(strippedInput))
    }
  }

  private def trainDictionary(input: DataSet[String]): DataSet[Map[String, Int]] = {
    val result = input.flatMap {
      text => {
        """\b\w+\b""".r.findAllIn(text).map(x => new Tuple1(x.toLowerCase))
      }
    }.distinct(0)
      .reduceGroup{
      (words, coll: Collector[Map[String, Int]]) => {
        val set = scala.collection.mutable.HashSet[String]()

        words foreach {
          word =>
            set += word._1
        }

        coll.collect(set.iterator.zipWithIndex.toMap)
      }
    }

    result
  }

  implicit val transformText = new TransformOperation[
      CountVectorizer,
      Map[String, Int],
      String,
      SparseVector]
  {/** Retrieves the model of the [[Transformer]] for which this operation has been defined.
    *
    * @param instance
    * @param transformParemters
    * @return
    */
  override def getModel(instance: CountVectorizer, transformParemters: ParameterMap):
  DataSet[Map[String, Int]] = {
    instance.dictionary match {
      case Some(dic) => dic
      case None => throw new RuntimeException("CountVectorizer was not trained.")
    }
  }

    /** Transforms a single element with respect to the model associated with the respective
      * [[Transformer]]
      *
      * @param element
      * @param model
      * @return
      */
    override def transform(element: String, model: Map[String, Int]): SparseVector = {
      transformTextElement(element, model)
    }
  }

  implicit val transformLabeledText = new TransformOperation[
      CountVectorizer,
      Map[String, Int],
      (Double, String),
      LabeledVector]
  {/** Retrieves the model of the [[Transformer]] for which this operation has
  been defined.
    *
    * @param instance
    * @param transformParemters
    * @return
    */
  override def getModel(instance: CountVectorizer, transformParemters: ParameterMap):
  DataSet[Map[String, Int]] = {
    instance.dictionary match {
      case Some(dic) => dic
      case None => throw new RuntimeException("CountVectorizer was not trained.")
    }
  }

    /** Transforms a single element with respect to the model associated with the respective
      * [[Transformer]]
      *
      * @param element
      * @param model
      * @return
      */
    override def transform(element: (Double, String), model: Map[String, Int]): LabeledVector = {
      LabeledVector(element._1, transformTextElement(element._2, model))
    }
  }

  private def transformTextElement(text: String, model: Map[String, Int]): SparseVector = {
    val coo = """\b\w+\b""".r.findAllIn(text).flatMap{
      word => {
        model.get(word.toLowerCase) match {
          case Some(id) => Some(id, 1.0)
          case None => None
        }
      }
    }

    SparseVector.fromCOO(model.size, coo.toIterable)
  }

  def apply(): CountVectorizer = {
    new CountVectorizer
  }
}
