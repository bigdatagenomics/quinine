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
import org.bdgenomics.formats.avro.{ AlignmentRecord, Contig }
import scala.math.abs

class ReadStatsSuite extends ADAMFunSuite {

  val qualThreshold = 20

  val contig1 = Contig.newBuilder()
    .setContigName("ctg1")
    .build()

  def qualString(qual: Int, length: Int): String = {
    // toChar.toString reads funny, but .toString on Int returns the int as a string literal
    // e.g.,
    // 'a'.toString = "a"
    // 'a'.toInt = 97
    // 'a'.toInt.toString = "97"
    // 'a'.toInt.toChar.toString = "a"
    (33 + qual).toChar.toString * length
  }

  // my, the perfect read is free of defects!
  val perfectRead = AlignmentRecord.newBuilder()
    .setSequence("A" * 10)
    .setQual(qualString(65, 10))
    .setDuplicateRead(false)
    .setFailedVendorQualityChecks(false)
    .setReadMapped(true)
    .setStart(1L)
    .setEnd(11L)
    .setContig(contig1)
    .setReadPaired(true)
    .setMateMapped(true)
    .setReadInFragment(0)
    .setMateContig(contig1)
    .setMateAlignmentStart(100L)
    .setMateAlignmentEnd(110L)
    .setMapq(65)
    .build()

  // similar to the perfect read, but has no qual
  val noQualRead = AlignmentRecord.newBuilder(perfectRead)
    .setQual(null)
    .build()

  // similar to the perfect read, but failed the vendor quality checks
  val failedVendorChecksRead = AlignmentRecord.newBuilder(perfectRead)
    .setFailedVendorQualityChecks(true)
    .build()

  // similar to the perfect read, but marked as a duplicate
  val duplicateRead = AlignmentRecord.newBuilder(perfectRead)
    .setDuplicateRead(true)
    .build()

  // similar to the perfect read, but all bases are low quality
  val lowBaseQualityRead = AlignmentRecord.newBuilder(perfectRead)
    .setQual(qualString(19, 10))
    .build()

  // similar to the perfect read, but the start and end bases are low quality
  val lowBaseQualityEndsRead = AlignmentRecord.newBuilder(perfectRead)
    .setQual(qualString(19, 1) + qualString(20, 8) + qualString(19, 1))
    .build()

  // similar to the perfect read, but with zero mapq
  val mapQZeroRead = AlignmentRecord.newBuilder(perfectRead)
    .setMapq(0)
    .build()

  // similar to the perfect read, but with mapq = 19
  val poorlyMappedRead = AlignmentRecord.newBuilder(perfectRead)
    .setMapq(19)
    .build()

  // similar to the perfect read, but overlaps its mate
  val overlappingFirstOfPairRead = AlignmentRecord.newBuilder(perfectRead)
    .setMateAlignmentStart(5L)
    .setMateAlignmentEnd(15L)
    .build()

  // similar to the overlapping first-of-pair read, but the second read in the template
  val overlappingSecondOfPairRead = AlignmentRecord.newBuilder(overlappingFirstOfPairRead)
    .setReadInFragment(1)
    .build()

  // similar to the perfect read, but without a pair
  val unpairedRead = AlignmentRecord.newBuilder(perfectRead)
    .setReadInFragment(null)
    .setReadPaired(false)
    .setMateContig(null)
    .setMateAlignmentStart(null)
    .setMateAlignmentEnd(null)
    .build()

  def stat(read: AlignmentRecord) = ReadStats(read, qualThreshold, qualThreshold)

  def genericAsserts(rs: ReadStats,
                     dupe: Boolean = false,
                     mapQZero: Boolean = false,
                     poorlyMapped: Boolean = false,
                     lowQualBases: Long = 0L,
                     overlapping: Boolean = false,
                     failedChecks: Boolean = false) {
    assert(rs.reads === 1L)
    assert(rs.bases === 10L)

    if (!failedChecks) {
      assert(rs.readsPassingVendorQualityFilter === 1L)
    } else {
      assert(rs.readsPassingVendorQualityFilter === 0L)
    }

    if (!dupe) {
      assert(rs.uniqueReads === 1L)
      assert(rs.duplicateBases === 0L)
    } else {
      assert(rs.uniqueReads === 0L)
      assert(rs.duplicateBases === 10L)
    }

    // dupe reads are not counted
    if (!mapQZero && !dupe) {
      assert(rs.nonZeroMapQReads === 1L)
      assert(rs.nonZeroMapQBases === 10L)
    } else {
      assert(rs.nonZeroMapQReads === 0L)
      assert(rs.nonZeroMapQBases === 0L)
    }

    if (!overlapping) {
      assert(rs.overlapBases === 0L)
    }

    if (!poorlyMapped && !mapQZero) {
      assert(rs.lowMapQBases === 0L)
    } else {
      assert(rs.lowMapQBases === 10L)
    }

    assert(rs.lowQualityBases === lowQualBases)
  }

  test("perfect read") {
    val rs = stat(perfectRead)

    genericAsserts(rs)
  }

  test("noQualRead should be equivalent to the perfect read, and should not NPE") {
    val rs = stat(noQualRead)

    genericAsserts(rs)
  }

  test("read failed vendor qc") {
    val rs = stat(failedVendorChecksRead)

    genericAsserts(rs, failedChecks = true)
  }

  test("read is a dupe") {
    val rs = stat(duplicateRead)

    genericAsserts(rs, dupe = true)
  }

  test("all of the bases in a read are low quality") {
    val rs = stat(lowBaseQualityRead)

    genericAsserts(rs, lowQualBases = 10L)
  }

  test("the start/end of the read have low quality scores") {
    val rs = stat(lowBaseQualityEndsRead)

    genericAsserts(rs, lowQualBases = 2L)
  }

  test("read's mapping quality is zero") {
    val rs = stat(mapQZeroRead)

    genericAsserts(rs, mapQZero = true)
  }

  test("read is mapped with low, but non-zero mapping quality") {
    val rs = stat(poorlyMappedRead)

    genericAsserts(rs, poorlyMapped = true)
  }

  test("first-of-pair read overlaps mate") {
    val rs = stat(overlappingFirstOfPairRead)

    genericAsserts(rs)
  }

  test("second-of-pair read overlaps mate") {
    val rs = stat(overlappingSecondOfPairRead)

    genericAsserts(rs, overlapping = true)
  }

  test("unpaired, but otherwise perfect read") {
    val rs = stat(unpairedRead)

    genericAsserts(rs)
  }

  test("merge two read stats instances") {
    val rs1 = ReadStats(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L)
    val rs2 = ReadStats(11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L, 19L, 20L)

    val mergedRs = rs1 ++ rs2

    assert(mergedRs.reads === 12L)
    assert(mergedRs.bases === 14L)
    assert(mergedRs.readsPassingVendorQualityFilter === 16L)
    assert(mergedRs.uniqueReads === 18L)
    assert(mergedRs.nonZeroMapQReads === 20L)
    assert(mergedRs.nonZeroMapQBases === 22L)
    assert(mergedRs.lowQualityBases === 24L)
    assert(mergedRs.duplicateBases === 26L)
    assert(mergedRs.lowMapQBases === 28L)
    assert(mergedRs.overlapBases === 30L)
  }

  sparkTest("run an aggregate across all reads") {
    val reads = Seq(perfectRead,
      failedVendorChecksRead,
      duplicateRead,
      lowBaseQualityRead,
      lowBaseQualityEndsRead,
      mapQZeroRead,
      poorlyMappedRead,
      overlappingFirstOfPairRead,
      overlappingSecondOfPairRead,
      unpairedRead)

    val rs = ReadStats(sc.parallelize(reads), qualThreshold, qualThreshold)

    assert(rs.reads === 10L)
    assert(rs.bases === 100L)
    assert(rs.readsPassingVendorQualityFilter === 9L)
    assert(rs.uniqueReads === 9L)
    assert(rs.nonZeroMapQReads === 8L)
    assert(rs.nonZeroMapQBases === 80L)
    assert(rs.lowQualityBases === 12L)
    assert(rs.duplicateBases === 10L)
    assert(rs.lowMapQBases === 20L)
    assert(rs.overlapBases === 10L)
    assert(fpCompare(rs.percentPassingChecks, 90.0))
    assert(fpCompare(rs.percentUniqueHighQualityReads, 80.0))
    assert(fpCompare(rs.percentLowQualityBases, 12.0))
    assert(fpCompare(rs.percentDuplicateBases, 10.0))
    assert(fpCompare(rs.percentLowMapQBases, 20.0))
    assert(fpCompare(rs.percentOverlapBases, 10.0))
  }

  def fpCompare(a: Double, b: Double, absTol: Double = 1e-3): Boolean = {
    abs(a - b) <= absTol
  }
}
