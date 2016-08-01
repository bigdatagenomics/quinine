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
package org.bdgenomics.quinine.metrics.insert

import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.{ AlignmentRecord, Contig }

class InsertSizeDistributionSuite extends ADAMFunSuite {

  val ctg1 = Contig.newBuilder()
    .setContigName("ctg1")
    .build()

  val ctg2 = Contig.newBuilder()
    .setContigName("ctg2")
    .build()

  val unpaired = AlignmentRecord.newBuilder()
    .setReadPaired(false)
    .build()

  val secondOfPair = AlignmentRecord.newBuilder()
    .setReadPaired(true)
    .setReadInFragment(1)
    .setReadMapped(true)
    .setPrimaryAlignment(true)
    .setContig(ctg1)
    .setStart(10L)
    .setEnd(50L)
    .setMateMapped(true)
    .setMateContig(ctg1)
    .setMateAlignmentStart(100L)
    .setMateAlignmentEnd(150L)
    .build()

  val unmappedFirstOfPair = AlignmentRecord.newBuilder()
    .setReadPaired(true)
    .setReadInFragment(0)
    .setReadMapped(false)
    .setMateMapped(true)
    .setMateContig(ctg1)
    .setMateAlignmentStart(100L)
    .setMateAlignmentEnd(150L)
    .build()

  val firstOfPairMateUnmapped = AlignmentRecord.newBuilder()
    .setReadPaired(true)
    .setReadInFragment(0)
    .setReadMapped(true)
    .setPrimaryAlignment(true)
    .setContig(ctg1)
    .setStart(100L)
    .setEnd(150L)
    .setMateMapped(false)
    .build()

  val secondaryFirstOfPair = AlignmentRecord.newBuilder()
    .setReadPaired(true)
    .setReadInFragment(0)
    .setReadMapped(true)
    .setPrimaryAlignment(false)
    .setContig(ctg1)
    .setStart(10L)
    .setEnd(50L)
    .setMateMapped(true)
    .setMateContig(ctg1)
    .setMateAlignmentStart(100L)
    .setMateAlignmentEnd(150L)
    .build()

  val chimericFragmentFirstOfPair = AlignmentRecord.newBuilder()
    .setReadPaired(true)
    .setReadInFragment(0)
    .setReadMapped(true)
    .setPrimaryAlignment(true)
    .setContig(ctg1)
    .setStart(10L)
    .setEnd(50L)
    .setMateMapped(true)
    .setMateContig(ctg2)
    .setMateAlignmentStart(100L)
    .setMateAlignmentEnd(150L)
    .build()

  val firstBeforeSecond = AlignmentRecord.newBuilder()
    .setReadPaired(true)
    .setReadInFragment(0)
    .setReadMapped(true)
    .setPrimaryAlignment(true)
    .setContig(ctg1)
    .setStart(10L)
    .setEnd(51L)
    .setMateMapped(true)
    .setMateContig(ctg1)
    .setMateAlignmentStart(100L)
    .setMateAlignmentEnd(151L)
    .build()

  val firstAfterSecond = AlignmentRecord.newBuilder()
    .setReadPaired(true)
    .setReadInFragment(0)
    .setReadMapped(true)
    .setPrimaryAlignment(true)
    .setContig(ctg1)
    .setStart(250L)
    .setEnd(301L)
    .setMateMapped(true)
    .setMateContig(ctg1)
    .setMateAlignmentStart(100L)
    .setMateAlignmentEnd(151L)
    .build()

  val badRead = AlignmentRecord.newBuilder()
    .setReadPaired(true)
    .setReadInFragment(0)
    .setReadMapped(true)
    .setPrimaryAlignment(true)
    .setMateMapped(true)
    .setMateContig(ctg1)
    .setMateAlignmentStart(100L)
    .setMateAlignmentEnd(151L)
    .build()

  val badMate = AlignmentRecord.newBuilder()
    .setReadPaired(true)
    .setReadInFragment(0)
    .setReadMapped(true)
    .setPrimaryAlignment(true)
    .setContig(ctg1)
    .setStart(250L)
    .setEnd(301L)
    .setMateMapped(true)
    .build()

  test("unpaired read should not return an insert size") {
    val insert = InsertSizeDistribution.insertSize(unpaired)
    assert(insert.isEmpty)
  }

  test("second of pair read should not return an insert size") {
    val insert = InsertSizeDistribution.insertSize(secondOfPair)
    assert(insert.isEmpty)
  }

  test("unmapped first of pair read should not return an insert size") {
    val insert = InsertSizeDistribution.insertSize(unmappedFirstOfPair)
    assert(insert.isEmpty)
  }

  test("first of pair read with unmapped mate should not return an insert size") {
    val insert = InsertSizeDistribution.insertSize(firstOfPairMateUnmapped)
    assert(insert.isEmpty)
  }

  test("first of pair read with non-primary alignment should not return an insert size") {
    val insert = InsertSizeDistribution.insertSize(secondaryFirstOfPair)
    assert(insert.isEmpty)
  }

  test("first of pair read from chimeric fragment should not return an insert size") {
    val insert = InsertSizeDistribution.insertSize(chimericFragmentFirstOfPair)
    assert(insert.isEmpty)
  }

  test("first of pair before second of pair") {
    val insert = InsertSizeDistribution.insertSize(firstBeforeSecond)
    assert(insert.isDefined)
    assert(insert.get === 50L)
  }

  test("first of pair after second of pair") {
    val insert = InsertSizeDistribution.insertSize(firstAfterSecond)
    assert(insert.isDefined)
    assert(insert.get === 100L)
  }

  test("malformed read should be ignored") {
    val insert = InsertSizeDistribution.insertSize(badRead)
    assert(insert.isEmpty)
  }

  test("malformed mate should be ignored") {
    val insert = InsertSizeDistribution.insertSize(badMate)
    assert(insert.isEmpty)
  }

  sparkTest("aggregate across reads") {
    val rdd = sc.parallelize(Seq(unpaired,
      secondOfPair,
      unmappedFirstOfPair,
      firstOfPairMateUnmapped,
      secondaryFirstOfPair,
      chimericFragmentFirstOfPair,
      firstBeforeSecond,
      firstAfterSecond,
      firstAfterSecond,
      firstAfterSecond,
      firstAfterSecond,
      badRead,
      badMate))
    val distribution = InsertSizeDistribution(rdd)
    assert(distribution.insertSizes.size === 2)
    assert(distribution.insertSizes(50L) === 1L)
    assert(distribution.insertSizes(100L) === 4L)
    assert(distribution.numFragments() === 5L)
    val mean = distribution.mean
    assert(mean > 89.999999 && mean < 90.000001)
  }
}
