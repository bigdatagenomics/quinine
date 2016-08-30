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

import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.Feature

class TranscriptCountsSuite extends ADAMFunSuite {

  val transcript1 = Feature.newBuilder()
    .setFeatureType("transcript")
    .setFeatureId("t1")
    .build()

  val transcript2 = Feature.newBuilder()
    .setFeatureType("SO:0000673")
    .setFeatureId("t2")
    .build()

  val misc = Feature.newBuilder()
    .setFeatureType("random")
    .build()

  sparkTest("run aggregate with one transcript") {
    val rdd = sc.parallelize(Seq(transcript1))
    val count = TranscriptCounts(rdd,
      rdd.map(Option(_)))

    assert(count.transcripts === 1L)
    assert(count.coveredTranscripts === 1L)
  }

  sparkTest("run aggregate with two transcripts, where one is covered twice") {
    val rdd1 = sc.parallelize(Seq(transcript1, transcript2, misc))
    val rdd2 = sc.parallelize(Seq(transcript1, transcript1))
      .map(Option(_))
    val count = TranscriptCounts(rdd1,
      rdd2)

    assert(count.transcripts === 2L)
    assert(count.coveredTranscripts === 1L)
  }

}
