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
package org.bdgenomics.quinine.metrics.coverage

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import scala.annotation.tailrec

/**
 * Simple statistics about coverage.
 *
 * @param median The median coverage of a set of sites.
 * @param mean The (arithmetic) mean coverage of a set of sites.
 * @param distribution A map between coverage and the number of sites covered
 *   at that coverage.
 */
case class CoverageStats(median: Double,
                         mean: Double,
                         distribution: Map[Int, Int]) {

  /**
   * The total number of sites, as a double.
   */
  private val sites = distribution.values
    .sum
    .toDouble

  /**
   * Returns the number of sites with coverage higher than a threshold.
   *
   * @param threshold The coverage threshold.
   * @return Returns the number of sites that are covered by more observations
   *   than this threshold value.
   *
   * @see percentSitesCoveredAt
   */
  def sitesCoveredAt(threshold: Int): Int = {
    distribution.filter(kv => kv._1 >= threshold)
      .values
      .sum
  }

  /**
   * Returns the percentage of sites with coverage higher than a threshold.
   *
   * @param threshold The coverage threshold.
   * @return Returns the percentage of sites that are covered by more observations
   *   than this threshold value.
   *
   * @see sitesCoveredAt
   */
  def percentSitesCoveredAt(threshold: Int): Double = {
    100.0 * sitesCoveredAt(threshold).toDouble / sites
  }
}

/**
 * Companion object for building coverage stats.
 */
object CoverageStats extends Serializable {

  /**
   * Takes the arithmetic mean of a Seq that contains (value, weight) pairs.
   *
   * @param distribution Seq of (value, weight) pairs.
   * @return The mean of this distribution.
   */
  private[coverage] def takeMean(distribution: Seq[(Int, Int)]): Double = {
    distribution.map(p => p._1 * p._2)
      .sum
      .toDouble / distribution.map(p => p._2)
      .sum
      .toDouble
  }

  /**
   * Takes the arithmetic mean of a Seq that contains (value, weight) pairs.
   *
   * @param distribution Seq of (value, weight) pairs.
   * @return The mean of this distribution.
   *
   * @note Assumes that the input is sorted by key.
   */
  private[coverage] def takeMedian(distribution: Seq[(Int, Int)]): Double = {

    if (distribution.isEmpty) {
      0.0
    } else {
      // sum the weights to get the total number of occurrences
      // divide this in half, and you have the median point
      val medianCount = (distribution.map(_._2).sum + 1).toDouble / 2.0

      @tailrec def recurse(iter: Iterator[(Int, Int)],
                           observed: Int,
                           lastIdx: Int,
                           optLower: Option[Int]): Double = {

        // if this triggers, something has gone terribly wrong
        // we should stop midway through the iterator
        // so, assume it passes, and pop the head
        assert(iter.hasNext, "Hit bad median at %d,%d in %s.".format(observed, lastIdx, distribution.mkString(",")))
        val (idx, count) = iter.next

        // we require that the list is sorted,
        // and thus that indices increase monotonically
        assert(idx > lastIdx)

        // where are we?
        if ((observed + 1).toDouble > medianCount) {

          // are we moving beyond the median?
          // if so, we should've seen that this was about to happen in the last step
          assert(optLower.isDefined)

          // if this happens, take the arithmetic mean of this and the last index
          (optLower.get + idx).toDouble / 2.0
        } else if (observed.toDouble <= medianCount && (observed + count).toDouble >= medianCount) {

          // are we centered around the median?
          // if so, return our current index
          idx.toDouble
        } else {

          // haven't hit the median yet! but,
          // is the median between us and the next step?
          val nextOptLower = if ((observed + count).toDouble < medianCount &&
            (observed + count + 1).toDouble > medianCount) {
            Some(idx)
          } else {
            None
          }

          // recurse!
          recurse(iter, observed + count, idx, nextOptLower)
        }
      }

      // compute the median
      recurse(distribution.toIterator, 0, -1, None)
    }
  }

  /**
   * Computes coverage stats from an RDD containing per-site coverage counts.
   *
   * @param rdd RDD containing tuples that describe the number of reads covering
   *   a given genomic locus/region.
   * @return Returns an instance describing the distribution of coverage over
   *   those loci.
   *
   * @tparam T A class that describes a genomic interval.
   */
  def apply[T <: ReferenceRegion](rdd: RDD[(T, Int)]): CoverageStats = {

    // compute coverage distribution, collect to driver, and sort
    val coverageDistribution = rdd.map(p => (p._2, 1))
      .reduceByKey(_ + _)
      .collect
      .toSeq
      .sortBy(_._1)

    // compute arithmetic mean of coverage
    val mean = takeMean(coverageDistribution)

    // compute median of coverage
    val median = takeMedian(coverageDistribution)

    // build and return
    CoverageStats(median, mean, coverageDistribution.toMap)
  }
}
