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

import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.{
  AlignmentRecord,
  Contig,
  Variant
}
import scala.math.{ abs, log }

class ContaminationEstimatorSuite extends ADAMFunSuite {

  val contig = Contig.newBuilder()
    .setContigName("ctg")
    .build()

  def buildRead(allele: Char): AlignmentRecord = {

    AlignmentRecord.newBuilder()
      .setContig(contig)
      .setStart(100L)
      .setEnd(101L)
      .setReadMapped(true)
      .setSequence(allele.toString)
      .setQual("5") // phred 20 --> p = 0.99
      .build()
  }

  sparkTest("estimate contamination from a read dataset with 10% contamination reads") {
    val contaminationVariant = VariantSite(Variant.newBuilder()
      .setContig(contig)
      .setStart(100L)
      .setEnd(101L)
      .setReferenceAllele("A")
      .setAlternateAllele("G")
      .build(), 0.0)
    val variantRdd = sc.parallelize(Seq(contaminationVariant))

    val reads = ((0 until 9).map(i => buildRead('A')) ++
      (0 until 1).map(i => buildRead('G')))
    val readRdd = sc.parallelize(reads)

    val ce = ContaminationEstimator(readRdd, variantRdd)
    val c = ce.estimateContamination()
      .mapContaminationEstimate()

    assert(c > 0.0999 && c < 0.1001)
  }

  sparkTest("estimate contamination from a read dataset with 5% contamination reads and misc other reads") {
    val contaminationVariant = VariantSite(Variant.newBuilder()
      .setContig(contig)
      .setStart(100L)
      .setEnd(101L)
      .setReferenceAllele("A")
      .setAlternateAllele("G")
      .build(), 0.0)
    val variantRdd = sc.parallelize(Seq(contaminationVariant))

    val reads = ((0 until 95).map(i => buildRead('A')) ++
      (0 until 5).map(i => buildRead('G')) ++
      (0 until 25).map(i => buildRead('T')))
    val readRdd = sc.parallelize(reads)

    val ce = ContaminationEstimator(readRdd, variantRdd)
    val c = ce.estimateContamination()
      .mapContaminationEstimate()

    assert(c > 0.0499 && c < 0.0501)
  }

  sparkTest("estimate contamination from a read dataset with 5% contamination reads and misc other reads including unmapped") {
    val contaminationVariant = VariantSite(Variant.newBuilder()
      .setContig(contig)
      .setStart(100L)
      .setEnd(101L)
      .setReferenceAllele("A")
      .setAlternateAllele("G")
      .build(), 0.0)
    val variantRdd = sc.parallelize(Seq(contaminationVariant))

    val reads = ((0 until 95).map(i => buildRead('A')) ++
      (0 until 5).map(i => buildRead('G')) ++
      (0 until 25).map(i => buildRead('T')) ++
      (0 until 100).map(i => AlignmentRecord.newBuilder()
        .setReadMapped(false)
        .build()))
    val readRdd = sc.parallelize(reads)

    val ce = ContaminationEstimator(readRdd, variantRdd)
    val c = ce.estimateContamination()
      .mapContaminationEstimate()

    assert(c > 0.0499 && c < 0.0501)
  }

  sparkTest("estimating contamination with no reads should throw an IAE") {
    val contaminationVariant = VariantSite(Variant.newBuilder()
      .setContig(contig)
      .setStart(100L)
      .setEnd(101L)
      .setReferenceAllele("A")
      .setAlternateAllele("G")
      .build(), 0.0)
    val variantRdd = sc.parallelize(Seq(contaminationVariant))

    val readRdd = sc.parallelize(Seq.empty[AlignmentRecord])

    val ce = ContaminationEstimator(readRdd, variantRdd)
    intercept[IllegalArgumentException] {
      val c = ce.estimateContamination()
        .mapContaminationEstimate()
    }
  }

  def fpLogCompare(a: Double, b: Double, tol: Double = 1e-3): Boolean = {
    val logB = log(b)
    abs(a - logB) <= tol
  }
}
