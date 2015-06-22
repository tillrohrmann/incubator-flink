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

package org.apache.flink.ml.regression

import org.apache.flink.api.scala._
import org.apache.flink.ml.common.{Parameter, ParameterMap}
import org.apache.flink.ml.pipeline.{PredictOperation, FitOperation, Predictor}
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.{Vector, DenseMatrix}

/**
 * Simple Least Squares Regression
 *
 * This implementation does the naive thing and uses parallel operations mainly
 * to compute the covariance matrix, and then resorts to normal (local)
 * linear algebra to compute the fit.
 *
 * The resulting function should work well up to a few thousand features.
 */
class SimpleLeastSquaresRegressionAlg(val C: Double) extends Serializable {

  private def XtimesY(dataset: DataSet[LabeledVector]): DataSet[Vector] = {
    dataset.map(xy => xy.vector * xy.label).reduce(_ + _)
  }

  private def covarianceMatrix(data: DataSet[Vector]): DataSet[DenseMatrix] = {
    data.map(x => x.outer(x)).reduce(_ + _)
  }

  def fit(dataset: DataSet[LabeledVector]): DataSet[Vector] = {
    val xs = dataset.map(_.vector)
    val c = covarianceMatrix(xs)
    val xy = XtimesY(dataset)

    val weights = c.cross(xy).map { cxy =>
      val (c, xy) = cxy
      (c + DenseMatrix.eye(c.numRows) * C).solve(xy)
    }

    weights
  }

  def predict(example: Vector, weights: Vector) = weights.dot(example)
}

class SimpleLeastSquaresRegression extends Predictor[SimpleLeastSquaresRegression] {
  var weights: Option[DataSet[Vector]] = None
}

object SimpleLeastSquaresRegression {
  case object Regularization extends Parameter[Double] {
    val defaultValue = Some(0.0)
  }

  implicit val fitLabelVectors = new FitOperation[SimpleLeastSquaresRegression, LabeledVector] {
    def fit(
        instance: SimpleLeastSquaresRegression,
        fitParameters: ParameterMap,
        input: DataSet[LabeledVector])
      : Unit = {
      val C = fitParameters.get(Regularization).get

      val alg = new SimpleLeastSquaresRegressionAlg(C)

      val weights = alg.fit(input)

      instance.weights = Some(weights)
    }
  }

  implicit def predictVectors[T <: Vector] =
    new PredictOperation[SimpleLeastSquaresRegression, Vector, T, Double]() {
      def getModel(
          instance: SimpleLeastSquaresRegression,
          predictParameters: ParameterMap)
        : DataSet[Vector] =
        instance.weights match {
          case Some(w) => w
          case None =>
            throw new RuntimeException("SimpleLeastSquaresRegression model not fit yet")
        }

      /** Calculates the prediction for a single element given the model of the [[Predictor]].
        *
        * @param value
        * @param model
        * @return
        */
      def predict(value: T, model: Vector): Double = {
        value.dot(model)
      }
    }
}
