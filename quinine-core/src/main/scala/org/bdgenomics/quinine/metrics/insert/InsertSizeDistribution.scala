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
package org.bdgenomics.quinine.metrics.insert

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.formats.avro.AlignmentRecord

/**
 * Helper object for computing the insert size distribution of reads sequenced
 * using a paired end sequencing protocol.
 */
private[quinine] object InsertSizeDistribution extends Serializable with Logging {

  /**
   * @param rdd An RDD of reads.
   * @return The insert size distribution across an RDD of reads.
   */
  def apply(rdd: RDD[AlignmentRecord]): InsertSizeDistribution = {
    InsertSizeDistribution(rdd.flatMap(insertSize)
      .countByValue
      .toMap)
  }

  /**
   * Computes the insert size of a single read.
   *
   * Computes the insert size of a read, as is used in the insert size
   * aggregator. This applies several filters before computing the insert
   * size:
   *
   * 1. The read must be paired, and be the first read from the pair.
   * 2. The read must be mapped, and it's mate must also be mapped.
   * 3. The two reads must be mapped to the same contig.
   *
   * These filters are necessary in order to compute the total insert size
   * distribution across all reads without duplicating reads from a given
   * fragment.
   *
   * @param read The read to compute the insert size of.
   * @return Optionally returns the insert size.
   */
  private[insert] def insertSize(read: AlignmentRecord): Option[Long] = {
    // we only return an insert size if:
    // 1. the read is paired
    // 2. the read is the first read from the pair
    // 3. the read is mapped
    // 4. the mapping is a primary alignment
    // 5. the read's mate is mapped
    // 6. the read and it's pair are mapped to the same contig
    try {
      if (read.getReadPaired &&
        read.getReadInFragment == 0 &&
        read.getReadMapped &&
        read.getPrimaryAlignment &&
        read.getMateMapped &&
        read.getContig.getContigName == read.getMateContig.getContigName) {
        ReferenceRegion(read).distance(ReferenceRegion(read.getMateContig.getContigName,
          read.getMateAlignmentStart,
          read.getMateAlignmentEnd))
      } else {
        None
      }
    } catch {
      case e: Throwable => {
        log.warn("Caught exception %s when processing read %s. Ignoring.".format(
          e, read))
        None
      }
    }
  }
}

/**
 * @param insertSizes A map between insert sizes and the number of times that
 *   a read pair was seen with that insert size.
 */
case class InsertSizeDistribution(insertSizes: Map[Long, Long]) {

  /**
   * @return Returns the number of fragments we observed.
   */
  private[insert] def numFragments(): Long = insertSizes.values.sum

  /**
   * @return Returns the mean insert size.
   */
  def mean(): Double = {
    insertSizes.map(kv => kv._1 * kv._2)
      .sum
      .toDouble / numFragments().toDouble
  }

  override def toString: String = {
    val distribution = insertSizes.toSeq
      .sortBy(_._1)
      .map(kv => "%d: %d".format(kv._1, kv._2))
      .mkString("\n")

    "Mean insert size: %3.2f\nInsert size distribution:\n%s".format(mean,
      distribution)
  }
}
