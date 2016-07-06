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

import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.{ AlignmentRecord, Contig }

class ReadCoverageSuite extends ADAMFunSuite {

  test("unmapped reads should emit no coverage observations") {
    val observations = ReadCoverage.flattenRead(AlignmentRecord.newBuilder()
      .setReadMapped(false)
      .build())

    assert(observations.isEmpty)
  }

  test("generate coverage observations for a mapped read") {
    val observations = ReadCoverage.flattenRead(AlignmentRecord.newBuilder()
      .setReadMapped(true)
      .setContig(Contig.newBuilder()
        .setContigName("chr")
        .build())
      .setStart(10L)
      .setEnd(15L)
      .build())

    assert(observations.size === 5)
    observations.foreach(p => assert(p._2 === 1))
    observations.foreach(p => assert(p._1.referenceName === "chr"))
    assert(observations.count(p => p._1.pos == 10L) === 1)
    assert(observations.count(p => p._1.pos == 11L) === 1)
    assert(observations.count(p => p._1.pos == 12L) === 1)
    assert(observations.count(p => p._1.pos == 13L) === 1)
    assert(observations.count(p => p._1.pos == 14L) === 1)
  }

  sparkTest("generate coverage observations for several mapped reads, and an unaligned read") {
    val ctg = Contig.newBuilder()
      .setContigName("chr")
      .build()

    val reads = (10 to 20).map(i => {
      AlignmentRecord.newBuilder()
        .setReadMapped(true)
        .setContig(ctg)
        .setStart(i.toLong)
        .setEnd(i.toLong + 5L)
        .build()
    }) ++ Seq(AlignmentRecord.newBuilder()
      .setReadMapped(false)
      .build())

    val observations = ReadCoverage(sc.parallelize(reads))
      .collect

    assert(observations.size === 15)
    assert(observations.count(i => i._1.referenceName == "chr") === 15)
    assert(observations.map(i => i._1.pos).toSet.size === 15)
    observations.map(i => i._1.pos)
      .foreach(p => assert(p >= 10L && p < 25L))
    assert(observations.filter(i => i._1.pos === 10L)
      .head._2 === 1)
    assert(observations.filter(i => i._1.pos === 11L)
      .head._2 === 2)
    assert(observations.filter(i => i._1.pos === 12L)
      .head._2 === 3)
    assert(observations.filter(i => i._1.pos === 13L)
      .head._2 === 4)
    assert(observations.filter(i => i._1.pos === 14L)
      .head._2 === 5)
    assert(observations.filter(i => i._1.pos === 15L)
      .head._2 === 5)
    assert(observations.filter(i => i._1.pos === 16L)
      .head._2 === 5)
    assert(observations.filter(i => i._1.pos === 17L)
      .head._2 === 5)
    assert(observations.filter(i => i._1.pos === 18L)
      .head._2 === 5)
    assert(observations.filter(i => i._1.pos === 19L)
      .head._2 === 5)
    assert(observations.filter(i => i._1.pos === 20L)
      .head._2 === 5)
    assert(observations.filter(i => i._1.pos === 21L)
      .head._2 === 4)
    assert(observations.filter(i => i._1.pos === 22L)
      .head._2 === 3)
    assert(observations.filter(i => i._1.pos === 23L)
      .head._2 === 2)
    assert(observations.filter(i => i._1.pos === 24L)
      .head._2 === 1)
  }
}
