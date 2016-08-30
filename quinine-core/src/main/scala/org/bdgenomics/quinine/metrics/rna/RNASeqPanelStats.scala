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
package org.bdgenomics.quinine.metrics.rna

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.BroadcastRegionJoin
import org.bdgenomics.formats.avro.{ AlignmentRecord, Feature }
import org.bdgenomics.quinine.metrics.coverage.{ CoverageStats, ReadCoverage }
import org.bdgenomics.quinine.rdd.RightOuterJoin

/**
 * Helper object for generating aggregate RNA-seq panel stats.
 */
object RNASeqPanelStats extends Serializable {

  /**
   * Generates aggregate count and coverage statistics for reads aligned to
   * a transcriptome build.
   *
   * @param reads Reads aligned to a specific transcriptome build.
   * @param transcriptome The transcriptome build, as loaded from a GTF/GFF or
   *   generic feature file.
   * @param startEndLength The amount of bases to consider as the 5'/3' end of
   *   the transcript.
   * @return Returns the aggregate stats.
   */
  def apply(reads: RDD[AlignmentRecord],
            transcriptome: RDD[Feature],
            startEndLength: Int): RNASeqPanelStats = {

    // cache the transcriptome build
    transcriptome.cache()

    // cache the reads
    reads.cache()

    // compute the raw read alignment stats
    val counts = RNASeqReadCounts(reads)

    // join the reads against the transcriptome description
    val joined = RightOuterJoin(transcriptome.keyBy(ReferenceRegion(_)),
      reads.filter(_.getReadMapped)
        .keyBy(ReferenceRegion(_)))
      .cache()

    // discard the alignments
    val featureOptRdd = joined.map(kv => kv._1)

    // run the alignment stat aggregator
    val alignmentStats = RNASeqAlignmentCounts(featureOptRdd)

    // run the transcript count bit
    val transcriptStats = TranscriptCounts(transcriptome, featureOptRdd)

    // compute coverage stats
    val coverageObservations = ReadCoverage(reads)
      .cache()

    // unpersist reads
    reads.unpersist()

    def isTranscript(f: Feature): Boolean = {
      f.getFeatureType == "transcript" || f.getFeatureType == "SO:0000673"
    }

    // join coverage data against transcript features
    val targetCoverageSites = BroadcastRegionJoin.partitionAndJoin(transcriptome.filter(isTranscript)
      .keyBy(ReferenceRegion(_)),
      coverageObservations.map(kv => {
        val (pos, obs) = kv
        (ReferenceRegion(pos), (pos, obs))
      }))
      .cache()

    // compute mean coverage per transcript
    val coverageStatsAllBases = CoverageStats(targetCoverageSites.map(kv => {
      val (_, (site, coverage)) = kv
      (site, coverage)
    }))

    // splits out the first/last _n_ bases of a reference region
    def fragment(rr: ReferenceRegion): Seq[ReferenceRegion] = {
      if (rr.length <= startEndLength) {
        Seq(rr)
      } else {
        Seq(ReferenceRegion(rr.referenceName, rr.start, rr.start + startEndLength),
          ReferenceRegion(rr.referenceName, rr.end - startEndLength, rr.end))
      }
    }

    // compute mean coverage at the ends of transcripts
    val coverageStatsStartEnd = CoverageStats(targetCoverageSites.flatMap(kv => {
      val (region, (site, coverage)) = kv

      // demorgan's law up in this
      // we keep the site if it overlaps with at least one of the start/end of a region
      // or, in other words, if it doesn't not overlap with all regions
      if (fragment(ReferenceRegion(region)).forall(r => !r.overlaps(site))) {
        None
      } else {
        Some((site, coverage))
      }
    }))

    RNASeqPanelStats(counts, alignmentStats, transcriptStats,
      coverageStatsAllBases, coverageStatsStartEnd)
  }
}

/**
 * Aggregated panel statistics.
 *
 * @param counts Alignment summary counts for this panel.
 * @param alignments Counts of read alignments to transcriptional regions.
 * @param transcripts Counts of the number of transcripts and the number of
 *   covered transcripts.
 * @param transcriptCoverage The coverage stats across all transcript bases.
 * @param startEndCoverage The coverage stats across the bases at the start/end
 *   of transcripts.
 */
case class RNASeqPanelStats(counts: RNASeqReadCounts,
                            alignments: RNASeqAlignmentCounts,
                            transcripts: TranscriptCounts,
                            transcriptCoverage: CoverageStats,
                            startEndCoverage: CoverageStats) {

  /**
   * @return Returns the panel metrics, pretty printed.
   */
  override def toString: String = {
    val lines: Seq[(String, Any, String)] = Seq(
      ("READS", counts.reads, "Total number of reads."),
      ("UNIQ", counts.unique, "Number of reads not marked as duplicate."),
      ("DUPE", counts.duplicate, "Number of reads marked as duplicates."),
      ("MAPPED", counts.mapped, "Number of reads that successfully aligned."),
      ("MAPUNI", counts.mappedUnique, "Number of reads that aligned and were not duplicates."),
      ("FWD", counts.forward, "Number of reads that aligned on the forward strand."),
      ("REV", counts.reverse, "Number of reads that aligned on the reverse strand."),
      ("INTER", alignments.intergenic, "Number of reads with intergenic alignments."),
      ("INTRA", alignments.intragenic, "Number of reads with intragenic alignments."),
      ("EXONIC", alignments.exonic, "Number of reads with exonic alignments."),
      ("INTRON", alignments.intronic, "Number of reads with intronic alignments."),
      ("RRNA", alignments.rRNA, "Number of reads that aligned to rRNA regions."),
      ("TRAN", transcripts.transcripts, "Number of transcripts."),
      ("CTRAN", transcripts.coveredTranscripts, "Transcripts covered by >1 read."),
      ("TCO", transcriptCoverage.mean, "Mean coverage per transcript."),
      ("SECO", startEndCoverage.mean, "Mean coverage at start/end of transcripts."))

    // convert lines to strings
    val asStrings = lines.map(t => "%s\t%s\t%s".format(t._1, t._2, t._3))

    def descriptor(d: String): Seq[String] = {
      Seq("%s %s %s".format("=" * 8, d, "=" * 8))
    }

    (descriptor("Read counts") ++ asStrings.take(7) ++
      descriptor("Alignments") ++ asStrings.drop(7).take(5) ++
      descriptor("Transcripts") ++ asStrings.drop(12).take(2) ++
      descriptor("Coverage") ++ asStrings.takeRight(2))
      .mkString("\n")
  }
}
