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
package org.bdgenomics.quinine.annotations

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.{
  Contig,
  Genotype,
  GenotypeAllele,
  Variant
}
import scala.collection.JavaConversions._

class AlleleFrequencySuite extends ADAMFunSuite {

  def buildDiploidGenotype(v: Variant,
                           alts: Int,
                           altAllele: GenotypeAllele = GenotypeAllele.Alt): Genotype = {
    require(alts <= 2 && alts >= 0)

    val alleles = ((0 until alts).map(i => altAllele) ++
      (alts until 2).map(i => GenotypeAllele.Ref))

    Genotype.newBuilder()
      .setVariant(v)
      .setAlleles(alleles)
      .build()
  }

  val ctg = Contig.newBuilder()
    .setContigName("ctg")
    .build()
  val v1 = Variant.newBuilder()
    .setContig(ctg)
    .setReferenceAllele("A")
    .setAlternateAllele("G")
    .setStart(1L)
    .setEnd(2L)
    .build()
  val v2 = Variant.newBuilder()
    .setContig(ctg)
    .setReferenceAllele("A")
    .setAlternateAllele("T")
    .setStart(1L)
    .setEnd(2L)
    .build()

  def buildGenotypeRdd(samples: Int, hets: Int, homAlts: Int): RDD[Genotype] = {
    require(hets + homAlts <= samples)

    sc.parallelize((0 until hets).map(i => buildDiploidGenotype(v1, 1)) ++
      (hets until (hets + homAlts)).map(i => buildDiploidGenotype(v1, 2)) ++
      ((hets + homAlts) until samples).map(i => buildDiploidGenotype(v1, 0)))
  }

  def testAf(samples: Int, hets: Int, homAlts: Int, expectedAf: Double) {
    val gRdd = buildGenotypeRdd(samples, hets, homAlts)

    val afs = AlleleFrequency.computeMinorAlleleFrequency(gRdd)
      .collect

    assert(afs.size === 1)
    assert(afs.head.getVariant === v1)

    val measuredAf = afs.head.getThousandGenomesAlleleFrequency
    assert(measuredAf > expectedAf - 0.001 && measuredAf < expectedAf + 0.001)
  }

  sparkTest("no alts at a site --> minor allele frequency = 0.0") {
    testAf(100, 0, 0, 0.0)
  }

  sparkTest("half hom ref, half hom alt --> minor allele frequency = 0.5") {
    testAf(100, 0, 50, 0.5)
  }

  sparkTest("half hom ref, half het --> minor allele frequency = 0.25") {
    testAf(100, 50, 0, 0.25)
  }

  sparkTest("70 hom ref, 20 het, 10 hom alt --> minor allele frequency = 0.2") {
    testAf(100, 20, 10, 0.2)
  }
}
