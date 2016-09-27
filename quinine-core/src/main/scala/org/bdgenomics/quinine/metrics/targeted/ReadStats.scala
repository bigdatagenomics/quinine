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
package org.bdgenomics.quinine.metrics.targeted

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.formats.avro.AlignmentRecord

/**
 * A class representing stats about a sequenced dataset.
 *
 * @param reads The number of reads in this dataset.
 * @param bases The number of bases in this dataset.
 * @param readsPassingVendorQualityFilter The number of reads that passed the
 *   vendor specific quality control filters.
 * @param uniqueReads The number of reads who have not been marked as a
 *   duplicate (typically, PCR or optical) of another read.
 * @param nonZeroMapQReads The number of reads who are uniquely mapped with
 *   non-zero mapping quality.
 * @param nonZeroMapQBases The number of sequenced bases that are uniquely
 *   mapped with non-zero mapping quality.
 * @param lowQualityBases The number of bases that were sequenced below a
 *   given quality threshold.
 * @param duplicateBases The number of bases contained in reads that were marked
 *   as duplicates.
 * @param lowMapQBases The number of bases contained in reads with a low mapping
 *   quality.
 * @param overlapBases The number of bases that are contained in reads that are
 *   not the first read from a sequenced fragment, and that overlap their mate.
 */
case class ReadStats(reads: Long,
                     bases: Long,
                     readsPassingVendorQualityFilter: Long,
                     uniqueReads: Long,
                     nonZeroMapQReads: Long,
                     nonZeroMapQBases: Long,
                     lowQualityBases: Long,
                     duplicateBases: Long,
                     lowMapQBases: Long,
                     overlapBases: Long) {

  /**
   * Total number of reads, as a double.
   *
   * @see reads
   */
  private lazy val totalReads = reads.toDouble

  /**
   * Total number of bases, as a double.
   *
   * @see bases
   */
  private lazy val totalBases = bases.toDouble

  /**
   * Helper function for returning the percent of reads out of total.
   *
   * @param num A long that symbolizes the number of reads that are some subset
   *   of the total set of reads.
   * @return How many reads out of the total set of reads are in this subset, as
   *   a percentage.
   */
  private def percentReads(num: Long): Double = {
    100.0 * num.toDouble / totalReads
  }

  /**
   * Helper function for returning the percent of bases out of total.
   *
   * @param num A long that symbolizes the number of bases that are some subset
   *   of the total set of bases.
   * @return How many bases out of the total set of bases are in this subset, as
   *   a percentage.
   */
  private def percentBases(num: Long): Double = {
    100.0 * num.toDouble / totalBases
  }

  /**
   * Merges together two descriptions of read stats by adding fields.
   *
   * @param that The stat bucket to merge with.
   * @return Returns a new ReadStats object.
   */
  def ++(that: ReadStats): ReadStats = {
    ReadStats(reads + that.reads,
      bases + that.bases,
      readsPassingVendorQualityFilter + that.readsPassingVendorQualityFilter,
      uniqueReads + that.uniqueReads,
      nonZeroMapQReads + that.nonZeroMapQReads,
      nonZeroMapQBases + that.nonZeroMapQBases,
      lowQualityBases + that.lowQualityBases,
      duplicateBases + that.duplicateBases,
      lowMapQBases + that.lowMapQBases,
      overlapBases + that.overlapBases)
  }

  /**
   * @return The percentage of reads that pass vendor quality checks.
   *
   * @see readsPassingVendorQualityFilter
   */
  def percentPassingChecks = percentReads(readsPassingVendorQualityFilter)

  /**
   * @return The percentage of reads that passed vendor quality checks, that are
   *   aligned with non-zero mapping quality, and that have not been flagged as
   *   duplicates.
   *
   * @see nonZeroMapQReads
   */
  def percentUniqueHighQualityReads = percentReads(nonZeroMapQReads)

  /**
   * @return The percentage of bases that have phred scores below a given
   *   quality score threshold.
   *
   * @see lowQualityBases
   */
  def percentLowQualityBases = percentBases(lowQualityBases)

  /**
   * @return The percentage of bases that are contained in reads that have been
   *   marked as duplicates.
   *
   * @see duplicateBases
   */
  def percentDuplicateBases = percentBases(duplicateBases)

  /**
   * @return The percentage of bases that are contained in reads that have been
   *   aligned with a low mapping quality.
   *
   * @see lowMapQBases
   */
  def percentLowMapQBases = percentBases(lowMapQBases)

  /**
   * @return The percentage of bases that are contained in reads where the read
   *   is from a fragment that produced multiple reads, where this read is not
   *   the first read in the fragment, and where the reads in the fragment are
   *   mapped with overlapping alignments.
   *
   * @see overlapBases
   */
  def percentOverlapBases = percentBases(overlapBases)
}

/**
 * Companion object with helper methods for working with [[AlignmentRecord]]s.
 */
private[targeted] object ReadStats extends Serializable {

  /**
   * Given a single read, extracts the statistics specific to this read.
   *
   * @param read Read to grab statistics for.
   * @param mapQThreshold Threshold for determining that a read is well mapped.
   * @param baseQualThreshold Threshold for determing that a base was sequenced
   *   correctly.
   * @return Returns the statistics for this single read.
   */
  def apply(read: AlignmentRecord,
            mapQThreshold: Int,
            baseQualThreshold: Int): ReadStats = {

    // how many bases are in this read?
    val readBases = read.getSequence
      .length
      .toLong

    // is this read marked as a duplicate?
    val (uniqueReads, duplicateBases,
      nonMapQZeroReads, nonMapQZeroBases) = if (read.getDuplicateRead) {
      (0L, readBases, 0L, 0L)
    } else {
      // what is the mapq of this read? are we a well aligned read?
      if (!read.getReadMapped || read.getMapq == 0) {
        (1L, 0L, 0L, 0L)
      } else {
        (1L, 0L, 1L, readBases)
      }
    }

    // how many bases have low quality? convert base qual to ascii first
    val asciiBaseQual = (baseQualThreshold + 33).toChar
    val lowQualityBases = Option(read.getQual).fold(0)(seq => seq.count(_ < asciiBaseQual))

    // is this read in a read pair with overlapping inserts?
    // we only care if we are not the first read from the fragment!
    val overlapBases = if (read.getReadMapped &&
      read.getReadPaired &&
      read.getReadInFragment != 0 &&
      read.getMateMapped) {

      // construct alignment models for our read pair
      val ourAlignment = ReferenceRegion(read)
      val pairAlignment = ReferenceRegion(read.getMateContig.getContigName,
        read.getMateAlignmentStart,
        read.getMateAlignmentStart + 1) // FIXME

      // do our alignments overlap?
      if (ourAlignment.overlaps(pairAlignment)) {
        readBases
      } else {
        0L
      }
    } else {
      0L
    }

    ReadStats(1L,
      readBases,
      if (read.getFailedVendorQualityChecks) 0L else 1L,
      uniqueReads,
      nonMapQZeroReads,
      nonMapQZeroBases,
      lowQualityBases,
      duplicateBases,
      if (read.getReadMapped && read.getMapq < mapQThreshold) readBases else 0L,
      overlapBases)
  }

  /**
   * Generates summary stats across an RDD of reads.
   *
   * @param rdd RDD of reads to generate statistics for.
   * @param mapQThreshold Threshold for determining that a read is well mapped.
   * @param baseQualThreshold Threshold for determing that a base was sequenced
   *   correctly.
   * @return Returns the aggregate statistics for this set of reads.
   */
  def apply(rdd: RDD[AlignmentRecord],
            mapQThreshold: Int,
            baseQualThreshold: Int): ReadStats = {
    rdd.map(apply(_, mapQThreshold, baseQualThreshold))
      .reduce(_ ++ _)
  }
}
