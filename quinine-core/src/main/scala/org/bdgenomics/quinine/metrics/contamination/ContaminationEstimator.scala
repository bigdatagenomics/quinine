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

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.BroadcastRegionJoin
import org.bdgenomics.formats.avro.{ Variant, AlignmentRecord }

/**
 * Estimates the cross-individual contamination in next-generating sequencing data,
 * using the ContEst algorithm (http://bioinformatics.oxfordjournals.org/content/27/18/2601)
 *
 * @param reads The set of reads used to estimate the contamination
 * @param variants A baseline set of variants (and their sites in the genome)
 */
class ContaminationEstimator(val reads: RDD[AlignmentRecord],
                             val variants: RDD[VariantSite]) {

  import ContaminationEstimator._

  /**
   * This is the main entry-point to the code -- it estimates a contamination parameter 'c'
   * by maximum-likelihood over a grid of possible values.
   *
   * @return The 'c' which maximizes the likelihood in the grid of possible values.
   */
  def estimateContamination(): Double = {
    val grid: Array[Double] = (0 until 100).map(i => 1.0 * i / 100.0).toArray
    val lls: Array[Double] = grid.map(logLikelihood)

    grid.zip(lls).maxBy(_._2)._1
  }

  /**
   * Calculates the log-likelihood of a particular contamination level, c, given the set of reads
   * and the set of variant sites.
   *
   * @param c The contamination level over which to calculate the log-likelihood
   * @return The log-likelihood
   */
  def logLikelihood(c: Double): Double = {
    val joined: RDD[(VariantSite, AlignmentRecord)] =
      BroadcastRegionJoin.partitionAndJoin(variants.keyBy(ContaminationEstimator.variantRegion), reads.keyBy(ReferenceRegion(_)))

    val readlikelihoods: RDD[(VariantSite, Double)] = joined.map(carry(calculateLogLikelihood(c)))
    val siteLikelihoods: RDD[(VariantSite, Double)] = readlikelihoods.reduceByKey(_ + _)

    siteLikelihoods.map(_._2).reduce(_ + _)
  }

  def calculateLogLikelihood(c: Double)(vs: VariantSite, read: AlignmentRecord): Double =
    vs.logLikelihood(c, read)

}

object ContaminationEstimator extends Serializable {

  /**
   * A specialized higher-order helper function -- this function, carry, takes a function
   * on pairs and produces a second function, on pairs, which produces a pair of the _first_
   * argument and the return value of the original (input) function.
   *
   * So if 'f' is a function such that f(1, 3) = 4, then carry(f)(1, 3) = (1, 4)
   *
   * @param f The binary function
   * @tparam U The type of the first argument to f
   * @tparam T The type of the second arguemnet to f
   * @tparam V The return type of f
   * @return a pair with type (U, V), whose first value is the first value of the input pair and
   *         whose second value is the result f(U, V) of calling the function on the input pair.
   */
  def carry[U, T, V](f: (U, T) => V): ((U, T)) => (U, V) = {
    def carried(p: (U, T)): (U, V) = {
      p match {
        case (u: U, _) => (u, f(p._1, p._2))
      }
    }
    carried
  }

  /**
   * Helper higher-order function, transforms a function into a function from pairs to
   * pairs, carrying through the original argument.
   *
   * So, if 'f' is a function such that f(1) = 10, then lift(f)(1) = (1, 10)
   *
   * In other words, the argument is carried through with the "original" result as a pair.
   *
   * @param f
   * @tparam U
   * @tparam T
   * @tparam V
   * @return
   */
  def lift[U, T, V](f: T => V): ((U, T)) => (U, V) = {
    def lifted(p: (U, T)): (U, V) = {
      p match {
        case (u: U, t: T) =>
          (u, f(t))
      }
    }
    lifted
  }

  def phredToError(phred: Char): Double = {
    val phredValue: Int = phred - '!'
    Math.exp(-10.0 * phredValue)
  }

  def readRegion(read: AlignmentRecord): ReferenceRegion = ReferenceRegion(read)
  def variantRegion(vs: VariantSite): ReferenceRegion =
    ReferenceRegion(vs.variant.getContig.getContigName, vs.variant.getStart, vs.variant.getEnd)
}

/**
 * In the ContEST paper, the variable 'i' indexes the sites of know variation, where
 * A_i is the reference allele (and \bar{A}_i the non-reference allele) at that site, and
 * f_i is the alternative (contaminating) population frequency of the variant.
 *
 * This class, VariantSite, encapsulates both the A_i and the f_i for a particular
 * variant site i.
 *
 * @param variant The variant with-respect-to-which we are calculating the likelihood of any read.
 * @param populationAlternateFrequency a frequency, specific to this Variant
 */
case class VariantSite(variant: Variant, populationAlternateFrequency: Double) extends Serializable {

  /**
   * Core likelihood function.
   *
   * The likelihood of a read, given the contamination estimate c.
   *
   * This is basically the third (unnumbered) equation from the first page of the ContEST paper.
   *
   * This code is adapted from the VariantSite.readLikelihood method in the jcontext package.
   *
   * @param c The contaminating fraction which is the primary parameter in the likelihood
   * @param read The read whose likelihood we are assessing.
   * @return The likelihood of the read given the contaminating fraction.
   */
  def logLikelihood(c: Double, read: AlignmentRecord): Double = {
    val offset: Int = (variant.getStart - read.getStart).toInt
    val readBase: String = read.getSequence.substring(offset, offset + 1)
    val error: Double = ContaminationEstimator.phredToError(read.getQual.charAt(offset))
    val notError = 1.0 - error
    val error3 = error / 3.0
    val f: Double = populationAlternateFrequency // f_i, but the i is implicit in this class

    if (readBase.equals(variant.getReferenceAllele)) {
      /*
      "if b_{ij} = A_i"
       */
      val not_c: Double = (1.0 - c) * notError
      val _c: Double = c * (f * notError + (1.0 - f) * error3)
      not_c + _c

    } else if (readBase.equals(variant.getAlternateAllele)) {
      /*
      "if b_{ij} = \bar{A}_i"
       */
      val not_c: Double = (1.0 - c) * error3
      val _c: Double = c * (f * error3 + (1.0 - f) * notError)
      not_c + _c

    } else {
      /*
      "otherwise"
       */
      error3
    }
  }
}
