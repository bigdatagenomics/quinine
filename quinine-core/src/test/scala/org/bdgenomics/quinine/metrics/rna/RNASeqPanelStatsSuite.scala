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
import org.bdgenomics.formats.avro.{ AlignmentRecord, Contig, Feature }

class RNASeqPanelStatsSuite extends ADAMFunSuite {

  val ctg = Contig.newBuilder()
    .setContigName("ctg")
    .build()

  def buildRead(start: Long, end: Long): AlignmentRecord = {
    AlignmentRecord.newBuilder()
      .setContig(ctg)
      .setStart(start)
      .setEnd(end)
      .setReadMapped(true)
      .setPrimaryAlignment(true)
      .setDuplicateRead(false)
      .build()
  }

  def buildTranscript(start: Long, end: Long, name: String): Feature = {
    Feature.newBuilder()
      .setContig(ctg)
      .setStart(start)
      .setEnd(end)
      .setFeatureType("transcript")
      .setFeatureId(name)
      .build()
  }

  def buildSequenceOntologyTranscript(start: Long, end: Long, name: String): Feature = {
    Feature.newBuilder()
      .setContig(ctg)
      .setStart(start)
      .setEnd(end)
      .setFeatureType("SO:0000673")
      .setFeatureId(name)
      .build()
  }

  sparkTest("single transcript, with reads at the start, middle, and end") {
    val reads = sc.parallelize(Seq(buildRead(100L, 200L),
      buildRead(200L, 300L),
      buildRead(300L, 400L)))
    val transcripts = sc.parallelize(Seq(buildTranscript(100L, 400L, "t1")))

    val stats = RNASeqPanelStats(reads, transcripts, 100)

    assert(stats.counts.reads === 3L)
    assert(stats.counts.unique === 3L)
    assert(stats.counts.duplicate === 0L)
    assert(stats.counts.mapped === 3L)
    assert(stats.counts.mappedUnique === 3L)
    assert(stats.counts.forward === 3L)
    assert(stats.counts.reverse === 0L)
    assert(stats.alignments.intergenic === 0L)
    assert(stats.alignments.intragenic === 3L)
    assert(stats.alignments.exonic === 0L)
    assert(stats.alignments.intronic === 0L)
    assert(stats.transcripts.transcripts === 1L)
    assert(stats.transcripts.coveredTranscripts === 1L)
    assert(stats.transcriptCoverage.median > 0.999 && stats.transcriptCoverage.median < 1.001)
    assert(stats.transcriptCoverage.mean > 0.999 && stats.transcriptCoverage.mean < 1.001)
    assert(stats.transcriptCoverage.distribution.size === 1)
    assert(stats.transcriptCoverage.distribution(1) === 300)
    assert(stats.startEndCoverage.median > 0.999 && stats.startEndCoverage.median < 1.001)
    assert(stats.startEndCoverage.mean > 0.999 && stats.startEndCoverage.mean < 1.001)
    assert(stats.startEndCoverage.distribution.size === 1)
    assert(stats.startEndCoverage.distribution(1) === 200)
  }

  sparkTest("two transcripts, one fully covered, one not covered at all, one random read") {
    val reads = sc.parallelize(Seq(buildRead(100L, 200L),
      buildRead(200L, 300L),
      buildRead(200L, 300L),
      buildRead(300L, 400L),
      buildRead(15000L, 15100L)))
    val transcripts = sc.parallelize(Seq(buildTranscript(100L, 400L, "t1"),
      buildTranscript(600L, 1000L, "t2")))

    val stats = RNASeqPanelStats(reads, transcripts, 100)

    assert(stats.counts.reads === 5L)
    assert(stats.counts.unique === 5L)
    assert(stats.counts.duplicate === 0L)
    assert(stats.counts.mapped === 5L)
    assert(stats.counts.mappedUnique === 5L)
    assert(stats.counts.forward === 5L)
    assert(stats.counts.reverse === 0L)
    assert(stats.alignments.intergenic === 1L)
    assert(stats.alignments.intragenic === 4L)
    assert(stats.alignments.exonic === 0L)
    assert(stats.alignments.intronic === 0L)
    assert(stats.transcripts.transcripts === 2L)
    assert(stats.transcripts.coveredTranscripts === 1L)
    assert(stats.transcriptCoverage.median > 0.999 && stats.transcriptCoverage.median < 1.001)
    assert(stats.transcriptCoverage.mean > 1.333 && stats.transcriptCoverage.mean < 1.334)
    assert(stats.transcriptCoverage.distribution.size === 2)
    assert(stats.transcriptCoverage.distribution(1) === 200)
    assert(stats.transcriptCoverage.distribution(2) === 100)
    assert(stats.startEndCoverage.median > 0.999 && stats.startEndCoverage.median < 1.001)
    assert(stats.startEndCoverage.mean > 0.999 && stats.startEndCoverage.mean < 1.001)
    assert(stats.startEndCoverage.distribution.size === 1)
    assert(stats.startEndCoverage.distribution(1) === 200)
  }

  sparkTest("two SO transcripts, one fully covered, one not covered at all, one random read") {
    val reads = sc.parallelize(Seq(buildRead(100L, 200L),
      buildRead(200L, 300L),
      buildRead(200L, 300L),
      buildRead(300L, 400L),
      buildRead(15000L, 15100L)))
    val transcripts = sc.parallelize(Seq(buildSequenceOntologyTranscript(100L, 400L, "t1"),
      buildTranscript(600L, 1000L, "t2")))

    val stats = RNASeqPanelStats(reads, transcripts, 100)

    assert(stats.counts.reads === 5L)
    assert(stats.counts.unique === 5L)
    assert(stats.counts.duplicate === 0L)
    assert(stats.counts.mapped === 5L)
    assert(stats.counts.mappedUnique === 5L)
    assert(stats.counts.forward === 5L)
    assert(stats.counts.reverse === 0L)
    assert(stats.alignments.intergenic === 1L)
    assert(stats.alignments.intragenic === 4L)
    assert(stats.alignments.exonic === 0L)
    assert(stats.alignments.intronic === 0L)
    assert(stats.transcripts.transcripts === 2L)
    assert(stats.transcripts.coveredTranscripts === 1L)
    assert(stats.transcriptCoverage.median > 0.999 && stats.transcriptCoverage.median < 1.001)
    assert(stats.transcriptCoverage.mean > 1.333 && stats.transcriptCoverage.mean < 1.334)
    assert(stats.transcriptCoverage.distribution.size === 2)
    assert(stats.transcriptCoverage.distribution(1) === 200)
    assert(stats.transcriptCoverage.distribution(2) === 100)
    assert(stats.startEndCoverage.median > 0.999 && stats.startEndCoverage.median < 1.001)
    assert(stats.startEndCoverage.mean > 0.999 && stats.startEndCoverage.mean < 1.001)
    assert(stats.startEndCoverage.distribution.size === 1)
    assert(stats.startEndCoverage.distribution(1) === 200)
  }
}
