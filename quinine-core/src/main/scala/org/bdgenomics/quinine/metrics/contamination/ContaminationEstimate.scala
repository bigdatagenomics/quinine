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
 * Class for representing contamination estimates.
 *
 * @param testedContaminations The percentage contamination values that we
 *   tested across.
 * @param completeLogLikelihoods The complete log likelihoods we got when we
 *   swept across the tested contaminations.
 *
 * @note Both input arrays must be the same length.
 */
case class ContaminationEstimate(testedContaminations: Seq[Double],
                                 completeLogLikelihoods: Seq[Double]) {

  // arrays must have the same length
  assert(testedContaminations.length == completeLogLikelihoods.length)

  /**
   * Constant value for 0.95 in log scale.
   *
   * Used for computing 95% confidence windows.
   */
  private val log95 = log(0.95)

  /**
   * The two arrays zipped together.
   */
  private val obs = testedContaminations.zip(completeLogLikelihoods)

  /**
   * @return Computes the maximum _a posteriori_ estimate of the contamination.
   */
  def mapContaminationEstimate(): Double = {

    // return the MAP contamination level estimate
    obs.maxBy(_._2)._1
  }

  /**
   * @return Returns a tuple containing the low and high ends of the 95%
   *   confidence window for the contamination estimate.
   */
  def confidenceWindow(): (Double, Double) = {

    // likelihoods above this value are within the 95% confidence window
    val confidenceLikelihood = obs.map(_._2).max + log95

    // so run a filter to get those points
    val confidentPoints = obs.filter(_._2 >= confidenceLikelihood)

    // take the high and low concentrations
    val lowConcentration = confidentPoints.minBy(_._1)._1
    val highConcentration = confidentPoints.maxBy(_._1)._1

    // and return!
    (lowConcentration, highConcentration)
  }

  override def toString: String = {
    val (low, high) = confidenceWindow()

    ("Maximum a posteriori contamination estimate: %f\n".format(mapContaminationEstimate) +
      "95%% confidence window: [%2.1f,%2.1f]\n\n".format(low, high) +
      "Tested contaminations and complete likelihoods:\n" +
      "CONTAM\tLOG L\n" +
      testedContaminations.zip(completeLogLikelihoods)
      .map(p => "%2.1f\t%1.3f".format(p._1, p._2))
      .mkString("\n"))
  }
}
