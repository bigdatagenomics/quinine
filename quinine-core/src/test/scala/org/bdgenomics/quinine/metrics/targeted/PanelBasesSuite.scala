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

import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.{ Contig, Feature }
import scala.math.abs

class PanelBasesSuite extends ADAMFunSuite {

  val contig = Contig.newBuilder()
    .setContigName("ctg")
    .build()

  sparkTest("compute coverage for a panel with one bait and one target") {

    // panel where target is 80bp and we have bait that goes 10bp off each end
    val bait = sc.parallelize(Seq(Feature.newBuilder()
      .setContig(contig)
      .setStart(100L)
      .setEnd(200L)
      .build()))

    val targets = sc.parallelize(Seq(Feature.newBuilder()
      .setContig(contig)
      .setStart(110L)
      .setEnd(190L)
      .build()))

    val bases = PanelBases(targets, bait)

    validate(bases)
  }

  sparkTest("compute coverage for a panel with two baits and one target") {

    // panel where target is 80bp
    // and we have two 50 bp baits that goes 10bp off each end
    val bait = sc.parallelize(Seq(Feature.newBuilder()
      .setContig(contig)
      .setStart(100L)
      .setEnd(150L)
      .build(),
      Feature.newBuilder()
        .setContig(contig)
        .setStart(150L)
        .setEnd(200L)
        .build()))

    val targets = sc.parallelize(Seq(Feature.newBuilder()
      .setContig(contig)
      .setStart(110L)
      .setEnd(190L)
      .build()))

    val bases = PanelBases(targets, bait)

    validate(bases)
  }

  sparkTest("compute coverage for a panel with two overlapping baits and one target") {

    // panel where target is 80bp
    // and we have two 55 bp baits that goes 10bp off each end and overlap by 10bp
    val bait = sc.parallelize(Seq(Feature.newBuilder()
      .setContig(contig)
      .setStart(100L)
      .setEnd(155L)
      .build(),
      Feature.newBuilder()
        .setContig(contig)
        .setStart(145L)
        .setEnd(200L)
        .build()))

    val targets = sc.parallelize(Seq(Feature.newBuilder()
      .setContig(contig)
      .setStart(110L)
      .setEnd(190L)
      .build()))

    val bases = PanelBases(targets, bait)

    validate(bases)
  }

  def validate(bases: PanelBases) {
    assert(bases.targetBases === 80L)
    assert(bases.uniqueBaitBases === 100L)
    assert(bases.offTargetBaitBases === 20L)
    assert(fpCompare(bases.percentOffTarget, 20.0))
    assert(fpCompare(bases.panelEfficiency, 0.8))
  }

  def fpCompare(a: Double, b: Double, absTol: Double = 1e-3): Boolean = {
    abs(a - b) <= absTol
  }
}
