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
import org.bdgenomics.formats.avro.AlignmentRecord

/**
 * Helper object for generating RNA-seq read count summaries.
 */
private[rna] object RNASeqReadCounts extends Serializable {

  /**
   * @param jBool A Java boolean value, which may be null.
   * @param default If the jBool was null, the default to fall back to.
   * @return Returns the Java boolean as a Scala boolean, with the null
   *   condition validated.
   */
  private def toScalaBool(jBool: java.lang.Boolean, default: Boolean = false): Boolean = {
    Option(jBool)
      .map(b => b: Boolean)
      .getOrElse(default)
  }

  /**
   * @param read The alignment record to generate counts for.
   * @return Fills in a read count object with the appropriate values determined
   *   by inspecting the alignment flags.
   */
  def apply(read: AlignmentRecord): RNASeqReadCounts = {

    val mappedFlag = toScalaBool(read.getReadMapped)

    val (unique, duplicate, mapped, mappedUnique) =
      (toScalaBool(read.getDuplicateRead), mappedFlag) match {
        case (true, true)   => (0L, 1L, 1L, 0L)
        case (true, false)  => (0L, 1L, 0L, 0L)
        case (false, true)  => (1L, 0L, 1L, 1L)
        case (false, false) => (1L, 0L, 0L, 0L)
      }

    val (forward, reverse) = (mappedFlag, toScalaBool(read.getReadNegativeStrand)) match {
      case (false, _)    => (0L, 0L)
      case (true, false) => (1L, 0L)
      case (true, true)  => (0L, 1L)
    }

    RNASeqReadCounts(1L,
      unique,
      duplicate,
      mapped,
      mappedUnique,
      forward,
      reverse)
  }

  /**
   * Computes read counts across an RDD of reads.
   *
   * @param rdd The RDD of reads to compute statistics for.
   * @return Returns a summary of the alignment info for the given panel.
   */
  def apply(rdd: RDD[AlignmentRecord]): RNASeqReadCounts = {
    rdd.map(apply)
      .reduce(_ ++ _)
  }
}

/**
 * Counts for the number of reads sequenced in an RNA-seq protocol after
 * alignment to a transcriptome build.
 *
 * @param reads The total number of reads.
 * @param unique The number of unique reads (not marked as duplicate).
 * @param duplicate The number of reads marked as duplicate.
 * @param mapped The number of reads that successfully aligned.
 * @param mappedUnique The number of reads that mapped successfully and that
 *   were not marked as duplicates.
 * @param forward Reads that were mapped on the forward strand.
 * @param reverse Reads that were mapped on the reverse strand.
 */
private[rna] case class RNASeqReadCounts(reads: Long,
                                         unique: Long,
                                         duplicate: Long,
                                         mapped: Long,
                                         mappedUnique: Long,
                                         forward: Long,
                                         reverse: Long) {

  def ++(that: RNASeqReadCounts): RNASeqReadCounts = {
    RNASeqReadCounts(reads + that.reads,
      unique + that.unique,
      duplicate + that.duplicate,
      mapped + that.mapped,
      mappedUnique + that.mappedUnique,
      forward + that.forward,
      reverse + that.reverse)
  }
}
