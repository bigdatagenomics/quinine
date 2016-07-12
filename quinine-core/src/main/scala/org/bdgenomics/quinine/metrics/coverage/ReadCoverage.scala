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

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferencePosition
import org.bdgenomics.formats.avro.AlignmentRecord

/**
 * Computes coverage at all sites covered in a sequencing dataset.
 */
object ReadCoverage extends Serializable {

  /**
   * Flatten a read into individual coverage observations.
   *
   * If a read is mapped, generates a Seq containing a tuple per position
   * covered by the read, where the tuple contains a ReferencePosition and the
   * integer 1. If the read is not mapped, returns an empty Seq.
   *
   * @param read The read to turn into coverage observations.
   * @return Returns (site, coverage) pairs if the read is mapped, or an empty
   *   Seq if the read is not mapped.
   */
  private[coverage] def flattenRead(read: AlignmentRecord): Seq[(ReferencePosition, Int)] = {
    if (read.getReadMapped) {
      (read.getStart.toInt until read.getEnd.toInt).map(idx => {
        (ReferencePosition(read.getContig.getContigName, idx), 1)
      })
    } else {
      Seq.empty
    }
  }

  /**
   * Given an RDD of reads, computes the coverage of all sites where at least
   * one read is aligned.
   *
   * @param reads An RDD of aligned reads.
   * @return Returns an RDD containing tuples of (genomic position, number of
   *   bases at this site).
   */
  def apply(reads: RDD[AlignmentRecord]): RDD[(ReferencePosition, Int)] = {
    reads.flatMap(flattenRead)
      .reduceByKey(_ + _)
  }
}
