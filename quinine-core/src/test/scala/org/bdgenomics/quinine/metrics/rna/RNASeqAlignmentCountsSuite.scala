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

class RNASeqAlignmentCountsSuite extends ADAMFunSuite {

  val transcript = Feature.newBuilder()
    .setFeatureType("transcript")
    .build()

  val exon = Feature.newBuilder()
    .setFeatureType("exon")
    .build()

  val intron = Feature.newBuilder()
    .setFeatureType("intron")
    .build()

  val misc = Feature.newBuilder()
    .setFeatureType("random")
    .build()

  val rRNA = Feature.newBuilder()
    .setFeatureType("rRNA")
    .build()

  val sequenceOntologyTranscript = Feature.newBuilder()
    .setFeatureType("SO:0000673")
    .build()

  val sequenceOntologyExon = Feature.newBuilder()
    .setFeatureType("SO:0000147")
    .build()

  val sequenceOntologyIntron = Feature.newBuilder()
    .setFeatureType("SO:0000188")
    .build()

  val sequenceOntologyMisc = Feature.newBuilder()
    .setFeatureType("SO:0000110")
    .build()

  val sequenceOntologyrRNA = Feature.newBuilder()
    .setFeatureType("SO:0000252")
    .build()

  def checkAndGet(countOpt: Option[RNASeqAlignmentCounts]): RNASeqAlignmentCounts = {
    assert(countOpt.isDefined)
    countOpt.get
  }

  test("should get an intergenic count if we pass a None") {
    val countOpt = RNASeqAlignmentCounts.fromOptFeature(None)

    val count = checkAndGet(countOpt)
    assert(count.intergenic === 1L)
    assert(count.intragenic === 0L)
    assert(count.exonic === 0L)
    assert(count.intronic === 0L)
    assert(count.rRNA === 0L)
  }

  def wrapAndCall(feature: Feature): RNASeqAlignmentCounts = {
    checkAndGet(RNASeqAlignmentCounts.fromOptFeature(Some(feature)))
  }

  test("should get an intragenic count if we pass a transcript") {
    val count = wrapAndCall(transcript)
    assert(count.intergenic === 0L)
    assert(count.intragenic === 1L)
    assert(count.exonic === 0L)
    assert(count.intronic === 0L)
    assert(count.rRNA === 0L)
  }

  test("should get an exonic count if we pass an exon") {
    val count = wrapAndCall(exon)
    assert(count.intergenic === 0L)
    assert(count.intragenic === 0L)
    assert(count.exonic === 1L)
    assert(count.intronic === 0L)
    assert(count.rRNA === 0L)
  }

  test("should get an intronic count if we pass an intron") {
    val count = wrapAndCall(intron)
    assert(count.intergenic === 0L)
    assert(count.intragenic === 0L)
    assert(count.exonic === 0L)
    assert(count.intronic === 1L)
    assert(count.rRNA === 0L)
  }

  test("should get an rRNA count if we pass an rRNA site") {
    val count = wrapAndCall(rRNA)
    assert(count.intergenic === 0L)
    assert(count.intragenic === 0L)
    assert(count.exonic === 0L)
    assert(count.intronic === 0L)
    assert(count.rRNA === 1L)
  }

  test("should get nothing if we pass a random feature") {
    val countOpt = RNASeqAlignmentCounts.fromOptFeature(Some(misc))
    assert(countOpt.isEmpty)
  }

  test("should get an intragenic count if we pass an SO transcript") {
    val count = wrapAndCall(sequenceOntologyTranscript)
    assert(count.intergenic === 0L)
    assert(count.intragenic === 1L)
    assert(count.exonic === 0L)
    assert(count.intronic === 0L)
    assert(count.rRNA === 0L)
  }

  test("should get an exonic count if we pass an SO exon") {
    val count = wrapAndCall(sequenceOntologyExon)
    assert(count.intergenic === 0L)
    assert(count.intragenic === 0L)
    assert(count.exonic === 1L)
    assert(count.intronic === 0L)
    assert(count.rRNA === 0L)
  }

  test("should get an intronic count if we pass an SO intron") {
    val count = wrapAndCall(sequenceOntologyIntron)
    assert(count.intergenic === 0L)
    assert(count.intragenic === 0L)
    assert(count.exonic === 0L)
    assert(count.intronic === 1L)
    assert(count.rRNA === 0L)
  }

  test("should get an rRNA count if we pass an SO rRNA site") {
    val count = wrapAndCall(sequenceOntologyrRNA)
    assert(count.intergenic === 0L)
    assert(count.intragenic === 0L)
    assert(count.exonic === 0L)
    assert(count.intronic === 0L)
    assert(count.rRNA === 1L)
  }

  test("should get nothing if we pass a random SO feature") {
    val countOpt = RNASeqAlignmentCounts.fromOptFeature(Some(sequenceOntologyMisc))
    assert(countOpt.isEmpty)
  }

  sparkTest("run aggregate") {
    val noFeature: Option[Feature] = None
    val rdd = sc.parallelize(Seq(noFeature, noFeature, noFeature) ++
      Seq(transcript, transcript, transcript, transcript,
        exon, exon,
        intron, intron,
        rRNA, rRNA, rRNA).map(s => Option(s)))
    val count = RNASeqAlignmentCounts(rdd)

    assert(count.intergenic === 3L)
    assert(count.intragenic === 4L)
    assert(count.exonic === 2L)
    assert(count.intronic === 2L)
    assert(count.rRNA === 3L)
  }
}
