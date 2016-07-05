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
import org.bdgenomics.formats.avro.{ AlignmentRecord, Feature }
import org.bdgenomics.quinine.metrics.coverage.{ CoverageStats, ReadCoverage }
import org.bdgenomics.quinine.rdd.RightOuterJoin

/**
 * Companion object for generating statistics about a targeted sequencing panel.
 */
object PanelStats extends Serializable {

  /**
   * Computes statistics for reads sequenced with a targeted sequencing protocol.
   *
   * @param reads The reads sequenced using this panel.
   * @param targets The areas targeted for sequencing using this panel.
   * @param bait The bait used for hybrid capture.
   * @param panelName The name of this panel.
   * @param baseQualThreshold The threshold used for characterizing a base as
   *   a high/low quality base.
   * @param mappingQualThreshold The threshold used for characterizing a read as
   *   being well/poorly mapped.
   * @return Returns a PanelStats instance that prints the statistics nicely.
   */
  def apply(reads: RDD[AlignmentRecord],
            targets: RDD[Feature],
            bait: RDD[Feature],
            panelName: String,
            baseQualThreshold: Int,
            mappingQualThreshold: Int): PanelStats = {

    // zip targets with uuids and then cache
    targets.cache()

    // compute the read statistics in a first pass
    val readStats = ReadStats(reads, baseQualThreshold, mappingQualThreshold)

    // compute coverage stats in a second pass, and cache
    val coverageObservations = ReadCoverage(reads)
      .cache()

    // compute target statistics
    val panelBaseStats = PanelBases(targets, bait)

    // join coverage counts against targets
    val targetCoverageSites = RightOuterJoin(targets.keyBy(ReferenceRegion(_)),
      coverageObservations.map(kv => {
        val (pos, obs) = kv
        (ReferenceRegion(pos), obs)
      }))

    // compute coverage per target by reducing over region uuids
    val perTargetCoverage = targetCoverageSites.flatMap(kv => {
      val (featureOpt, coverage) = kv

      featureOpt.map(feature => {
        // get the region covered by a feature
        val reg = ReferenceRegion(feature)

        (reg, coverage)
      })
    }).reduceByKey(_ + _)
      .map(kv => {
        val (region, coverage) = kv

        // normalize by site
        (region, (coverage.toDouble / region.width.toDouble).toInt)
      })

    // get stats over site
    val targetCoverageStats = CoverageStats(perTargetCoverage)

    // get the count of bases that did not map to a target
    val offTargetBases = targetCoverageSites.flatMap(kv => {
      val (featureOpt, coverage) = kv

      if (featureOpt.isEmpty) {
        Some(coverage)
      } else {
        None
      }
    }).reduce(_ + _)

    // construct the panel stats class
    new PanelStats(panelName, readStats, targetCoverageStats, panelBaseStats)
  }
}

/**
 * A convenience class that holds the computed statistics.
 *
 * @param panelName The name of this targeted sequencing panel.
 * @param readStats General statistics about the reads aligned to this panel.
 */
case class PanelStats private (panelName: String,
                               readStats: ReadStats,
                               targetCoverageStats: CoverageStats,
                               panelBaseStats: PanelBases) {

  override def toString: String = {

    // compute the coverage percentages
    val coverageLines = Seq(1, 2, 10, 20, 30, 40, 50, 100).map(c => {
      ("%%C%d".format(c),
        "The percentage of targeted bases covered at %d or higher.".format(c),
        targetCoverageStats.percentSitesCoveredAt(c).toInt)
    })

    // build the output lines we're going to write
    val outputLines: Seq[(String, String, Any)] = Seq(
      ("PANEL", "The panel name.", panelName),
      ("READS", "The number of reads.", readStats.reads),
      ("BASES", "The number of sequenced bases.", readStats.bases),
      ("PQCR", "The number of reads that passed vendor quality checks.",
        readStats.readsPassingVendorQualityFilter),
      ("UPQR", "The number of unique (not-duplicate) reads that passed vendor QC.",
        readStats.uniqueReads),
      ("MQ0R", "The number of UPQR reads that had non-zero mapping quality.",
        readStats.nonZeroMapQReads),
      ("MQ0B", "The number of bases across all MQ0R reads.",
        readStats.nonZeroMapQBases),
      ("LQB", "The number of bases that were below a quality score threshold.",
        readStats.lowQualityBases),
      ("DUPEB", "The number of bases in reads marked as duplicates.",
        readStats.duplicateBases),
      ("LOWMQB", "The number of bases in reads mapped with MAPQ below a threshold.",
        readStats.lowMapQBases),
      ("OLB", "The number of bases in second-of-pair reads that overlap with their mate.",
        readStats.overlapBases),
      ("%QCR", "The percentage of reads that passed vendor QC.",
        "%d%%".format(readStats.percentPassingChecks.toInt)),
      ("%UPQR", "The percentage of unique reads that passed vendor QC.",
        "%d%%".format(readStats.percentUniqueHighQualityReads.toInt)),
      ("%LQB", "The percentage of bases that had low quality scores.",
        "%d%%".format(readStats.percentLowQualityBases.toInt)),
      ("%DB", "The percentage of bases that are in reads marked as duplicates.",
        "%d%%".format(readStats.percentDuplicateBases.toInt)),
      ("%LMQB", "The percentage of bases that are in reads with a low mapping quality.",
        "%d%%".format(readStats.percentLowMapQBases.toInt)),
      ("%OLB", "The percentage of bases that are in second-of-pair reads whose alignment overlaps their mate.",
        "%d%%".format(readStats.percentOverlapBases.toInt)),
      ("MEDIAN", "The median target coverage depth.",
        targetCoverageStats.median),
      ("MEAN", "The (arithmetic) mean target coverage depth.",
        targetCoverageStats.mean),
      ("PANELB", "The number of bases targeted by this panel.",
        panelBaseStats.targetBases),
      ("BAITB", "The number of bases covered by one or more bait.",
        panelBaseStats.uniqueBaitBases),
      ("%OFT", "The percentage of baited bases that do not cover a targeted base.",
        "%s%%".format(panelBaseStats.percentOffTarget.toInt)),
      ("EFF", "The efficiency of the panel: the fraction of baited bases that cover a targeted base.",
        panelBaseStats.panelEfficiency)) ++ coverageLines

    ("Metric\tValue\tDescription\n" +
      outputLines.map(t => {
        "%s\t%s\t%s".format(t._1, t._3, t._2)
      }).mkString("\n"))
  }
}
