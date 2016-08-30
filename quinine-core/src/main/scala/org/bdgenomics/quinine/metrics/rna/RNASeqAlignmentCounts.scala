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

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.bdgenomics.formats.avro.Feature

private[rna] object RNASeqAlignmentCounts extends Serializable with Logging {

  /**
   * A count object for a read that has aligned outside of a gene.
   */
  private val intergenic: Option[RNASeqAlignmentCounts] =
    Some(RNASeqAlignmentCounts(1L, 0L, 0L, 0L, 0L))

  /**
   * Creates count objects for reads that have aligned inside of a gene.
   *
   * Identifies whether the read has aligned to an intragenic area, or more
   * specifically to an exon or intron.
   *
   * @param feature The feature we've aligned to.
   * @return Returns the appropriately populated intermediate count record.
   */
  private def intragenic(feature: Feature): Option[RNASeqAlignmentCounts] = {
    feature.getFeatureType match {
      case "transcript" | "SO:0000673" => Some(RNASeqAlignmentCounts(0L, 1L, 0L, 0L, 0L))
      case "exon" | "SO:0000147"       => Some(RNASeqAlignmentCounts(0L, 0L, 1L, 0L, 0L))
      case "intron" | "SO:0000188"     => Some(RNASeqAlignmentCounts(0L, 0L, 0L, 1L, 0L))
      case "rRNA" | "SO:0000252"       => Some(RNASeqAlignmentCounts(0L, 0L, 0L, 0L, 1L))
      case s: String => {
        log.warn("Found unknown feature type %s. Ignoring.".format(s))
        None
      }
    }
  }

  /**
   * @param optFeature The feature that a read was optionally aligned against.
   * @return Returns the appropriate intermediate count.
   */
  def fromOptFeature(optFeature: Option[Feature]): Option[RNASeqAlignmentCounts] = {
    optFeature.fold(intergenic)(f => intragenic(f))
  }

  /**
   * Calculates panel alignment statistics across a set of feature alignments.
   *
   * @param featureRdd The optional features that a set of RNA-seq reads aligned
   *   to. These should be obtained by joining RNA-seq reads against a
   *   transcriptome description.
   * @return Returns the summary counts of alignments to various features.
   */
  def apply(featureRdd: RDD[Option[Feature]]): RNASeqAlignmentCounts = {
    featureRdd.flatMap(fromOptFeature)
      .reduce(_ ++ _)
  }
}

/**
 * @param intergenic Reads that were mapped between genes.
 * @param intragenic Reads that were mapped within genes.
 * @param exonic Reads that mapped to an exon.
 * @param intronic Reads that mapped to an intron.
 * @param rRNA Reads that mapped to a location labeled as rRNA.
 */
private[rna] case class RNASeqAlignmentCounts(intergenic: Long,
                                              intragenic: Long,
                                              exonic: Long,
                                              intronic: Long,
                                              rRNA: Long) {

  def ++(that: RNASeqAlignmentCounts): RNASeqAlignmentCounts = {
    RNASeqAlignmentCounts(intergenic + that.intergenic,
      intragenic + that.intragenic,
      exonic + that.exonic,
      intronic + that.intronic,
      rRNA + that.rRNA)
  }

  /**
   * @param totalReads The total number of reads in this dataset.
   * @return Returns the ratio of exon mapped reads to total reads.
   */
  def profileEfficiency(totalReads: Long): Double = {
    exonic.toDouble / totalReads.toDouble
  }
}
