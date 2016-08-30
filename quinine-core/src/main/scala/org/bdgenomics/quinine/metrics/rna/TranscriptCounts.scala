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
import org.bdgenomics.formats.avro.Feature

private[rna] object TranscriptCounts extends Serializable {

  /**
   * @param transcriptomeRdd A RDD of features that describe a given reference
   *   transcriptome.
   * @param featureRdd The optional features that a set of RNA-seq reads aligned
   *   to. These should be obtained by joining RNA-seq reads against a
   *   transcriptome description.
   * @return Returns the number of transcripts, and the number of transcripts
   *   that have at least one read aligned to them.
   */
  def apply(transcriptomeRdd: RDD[Feature],
            featureRdd: RDD[Option[Feature]]): TranscriptCounts = {

    def isTranscript(f: Feature): Boolean = {
      f.getFeatureType == "transcript" || f.getFeatureType == "SO:0000673"
    }

    // count the number of transcripts
    val transcripts = transcriptomeRdd.filter(isTranscript)
      .count

    // get the number of transcripts where there is at least one read
    // by filtering on transcripts and calling distinct
    val coveredTranscripts = featureRdd.flatMap(opt => {
      opt.filter(isTranscript)
        .map(f => f.getFeatureId)
    }).distinct
      .count

    TranscriptCounts(transcripts, coveredTranscripts)
  }
}

/**
 * @param transcripts The total number of transcripts.
 * @param coveredTranscripts The number of transcripts covered by at least one
 *   read.
 */
private[rna] case class TranscriptCounts(transcripts: Long,
                                         coveredTranscripts: Long) {
}
