/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
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
package org.bdgenomics.quinine.metrics.contamination

import scala.math.log

/**
 * A trait describing a base in a read that covered a variant site.
 */
private[contamination] sealed trait Observation {

  /**
   * The probability that this base was sequenced incorrectly.
   */
  val baseErrorProbability: Double

  /**
   * The probability that this base was sequenced correctly.
   */
  val baseSuccessProbability: Double = 1.0 - baseErrorProbability

  /**
   * The probability that this base was sequenced incorrectly, divided by three.
   *
   * For a uniform substitution error model, the probability of seeing a
   * given base, given that the original base was missequenced, is one third of
   * the base error probability.
   */
  val errorDiv3 = baseErrorProbability / 3.0

  /**
   * The frequency of the alternate allele.
   */
  val minorAlleleFrequency: Double

  /**
   * The frequency of the reference allele.
   */
  val majorAlleleFrequency: Double = 1.0 - minorAlleleFrequency

  /**
   * @param contamination The estimated inter-sample contamination.
   * @return Returns the log likelihood of this contamination estimate given the
   *   observed read.
   */
  def logLikelihood(contamination: Double): Double
}

/**
 * Derived from a read which covers a variant site and observes the reference.
 *
 * @param baseErrorProbability The probability that this base was sequenced
 *   incorrectly.
 * @param minorAlleleFrequency The frequency of the alternate allele.
 */
private[contamination] case class ReferenceObservation(
    baseErrorProbability: Double,
    minorAlleleFrequency: Double) extends Observation {

  /**
   * Returns the log likelihood of observing the reference allele.
   *
   * L(c | a = A, f, \epsilon) = (1 - c) * (1 - \epsilon) +
   *                              c * (f * (1 - \epsilon) +
   *                                   (1 - f) * \epsilon / 3)
   *
   * @param contamination The estimated inter-sample contamination.
   * @return Returns the log likelihood of this contamination estimate given the
   *   observed read that matches the reference.
   */
  def logLikelihood(contamination: Double): Double = {
    log(((1.0 - contamination) * baseSuccessProbability +
      (contamination * (minorAlleleFrequency * baseSuccessProbability +
        majorAlleleFrequency * errorDiv3))))
  }
}

/**
 * Derived from a read which covers a variant site and observes the alternate.
 *
 * @param baseErrorProbability The probability that this base was sequenced
 *   incorrectly.
 * @param minorAlleleFrequency The frequency of the alternate allele.
 */
private[contamination] case class AlternateObservation(
    baseErrorProbability: Double,
    minorAlleleFrequency: Double) extends Observation {

  /**
   * Returns the log likelihood of observing the alternate allele.
   *
   * L(c | a = \bar{A}, f, \epsilon) = (1 - c) * \epsilon / 3 +
   *                                    c * (f * \epsilon / 3 +
   *                                         (1 - f) * (1 - \epsilon))
   *
   * @param contamination The estimated inter-sample contamination.
   * @return Returns the log likelihood of this contamination estimate given the
   *   observed read matching the alternate allele.
   */
  def logLikelihood(contamination: Double): Double = {
    log(((1.0 - contamination) * errorDiv3 +
      (contamination * (minorAlleleFrequency * errorDiv3 +
        majorAlleleFrequency * baseSuccessProbability))))
  }
}
