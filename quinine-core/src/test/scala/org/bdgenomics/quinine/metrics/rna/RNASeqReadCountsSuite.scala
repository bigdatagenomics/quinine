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
import org.bdgenomics.formats.avro.AlignmentRecord

class RNASeqReadCountsSuite extends ADAMFunSuite {

  val unalignedRead = AlignmentRecord.newBuilder()
    .setReadMapped(false)
    .build()

  val fwdUniqueMappedRead = AlignmentRecord.newBuilder()
    .setReadMapped(true)
    .setDuplicateRead(false)
    .setReadNegativeStrand(false)
    .build()

  val revUniqueMappedRead = AlignmentRecord.newBuilder()
    .setReadMapped(true)
    .setDuplicateRead(false)
    .setReadNegativeStrand(true)
    .build()

  val fwdDupeMappedRead = AlignmentRecord.newBuilder()
    .setReadMapped(true)
    .setDuplicateRead(true)
    .setReadNegativeStrand(false)
    .build()

  val revDupeMappedRead = AlignmentRecord.newBuilder()
    .setReadMapped(true)
    .setDuplicateRead(true)
    .setReadNegativeStrand(true)
    .build()

  test("check stats for an unaligned read") {
    val stats = RNASeqReadCounts(unalignedRead)

    assert(stats.reads === 1L)
    assert(stats.unique === 1L)
    assert(stats.duplicate === 0L)
    assert(stats.mapped === 0L)
    assert(stats.mappedUnique === 0L)
    assert(stats.forward === 0L)
    assert(stats.reverse === 0L)
  }

  test("check stats for an unique read, mapped on the forward strand") {
    val stats = RNASeqReadCounts(fwdUniqueMappedRead)

    assert(stats.reads === 1L)
    assert(stats.unique === 1L)
    assert(stats.duplicate === 0L)
    assert(stats.mapped === 1L)
    assert(stats.mappedUnique === 1L)
    assert(stats.forward === 1L)
    assert(stats.reverse === 0L)
  }

  test("check stats for an unique read, mapped on the reverse strand") {
    val stats = RNASeqReadCounts(revUniqueMappedRead)

    assert(stats.reads === 1L)
    assert(stats.unique === 1L)
    assert(stats.duplicate === 0L)
    assert(stats.mapped === 1L)
    assert(stats.mappedUnique === 1L)
    assert(stats.forward === 0L)
    assert(stats.reverse === 1L)
  }

  test("check stats for a duplicated read, mapped on the forward strand") {
    val stats = RNASeqReadCounts(fwdDupeMappedRead)

    assert(stats.reads === 1L)
    assert(stats.unique === 0L)
    assert(stats.duplicate === 1L)
    assert(stats.mapped === 1L)
    assert(stats.mappedUnique === 0L)
    assert(stats.forward === 1L)
    assert(stats.reverse === 0L)
  }

  test("check stats for a duplicated read, mapped on the reverse strand") {
    val stats = RNASeqReadCounts(revDupeMappedRead)

    assert(stats.reads === 1L)
    assert(stats.unique === 0L)
    assert(stats.duplicate === 1L)
    assert(stats.mapped === 1L)
    assert(stats.mappedUnique === 0L)
    assert(stats.forward === 0L)
    assert(stats.reverse === 1L)
  }

  sparkTest("run simple roll up") {
    val rdd = sc.parallelize(Seq(unalignedRead,
      fwdUniqueMappedRead, revUniqueMappedRead,
      fwdDupeMappedRead, revDupeMappedRead))

    val stats = RNASeqReadCounts(rdd)

    assert(stats.reads === 5L)
    assert(stats.unique === 3L)
    assert(stats.duplicate === 2L)
    assert(stats.mapped === 4L)
    assert(stats.mappedUnique === 2L)
    assert(stats.forward === 2L)
    assert(stats.reverse === 2L)
  }
}
