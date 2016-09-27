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
package org.bdgenomics.quinine.metrics.contamination

import org.bdgenomics.formats.avro.{
  AlignmentRecord,
  Contig,
  Variant
}
import org.scalatest.FunSuite
import scala.math.{ abs, log }

class VariantSiteSuite extends FunSuite {

  val contig = Contig.newBuilder()
    .setContigName("ctg")
    .build()

  val variant = VariantSite(Variant.newBuilder()
    .setContig(contig)
    .setStart(100L)
    .setEnd(101L)
    .setReferenceAllele("A")
    .setAlternateAllele("G")
    .build(), 0.3)

  def buildRead(allele: Char): AlignmentRecord = {

    AlignmentRecord.newBuilder()
      .setContig(contig)
      .setStart(100L)
      .setEnd(101L)
      .setSequence(allele.toString)
      .setQual("5") // phred 20 --> p = 0.99
      .build()
  }

  test("read overlaps a site and agrees with the reference") {
    val read = buildRead('A')

    val optObs = variant.toObservation(read)

    optObs match {
      case Some(obs: ReferenceObservation) => {
        val ll = obs.logLikelihood(0.1)

        assert(fpLogCompare(ll, (0.9 * 0.99) + 0.1 * (0.3 * 0.99 + 0.7 * 0.01 / 3.0)))
      }
      case _ => assert(false)
    }
  }

  test("read overlaps a site and agrees with the alt") {
    val read = buildRead('G')

    val optObs = variant.toObservation(read)

    optObs match {
      case Some(obs: AlternateObservation) => {
        val ll = obs.logLikelihood(0.1)

        assert(fpLogCompare(ll, (0.9 * 0.01 / 3.0) + 0.1 * (0.3 * 0.01 / 3.0 + 0.7 * 0.99)))
      }
      case _ => assert(false)
    }
  }

  test("read overlaps a site and is an other-alt") {
    val read = buildRead('T')

    val optObs = variant.toObservation(read)

    assert(optObs.isEmpty)
  }

  test("read has no bases and should fail on conversion") {
    val read = AlignmentRecord.newBuilder()
      .setContig(contig)
      .setStart(100L)
      .setEnd(101L)
      .setSequence("")
      .setQual("")
      .build()

    val optObs = variant.toObservation(read)

    assert(optObs.isEmpty)
  }

  def fpLogCompare(a: Double, b: Double, tol: Double = 1e-3): Boolean = {
    val logB = log(b)
    abs(a - logB) <= tol
  }
}
