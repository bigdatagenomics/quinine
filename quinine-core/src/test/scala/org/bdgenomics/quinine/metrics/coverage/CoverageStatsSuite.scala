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

class CoverageStatsSuite extends ADAMFunSuite {

  test("take simple mean of all equally weighted points") {
    val obs = Seq((0, 1), (1, 1), (2, 1), (3, 1), (4, 1))

    val mean = CoverageStats.takeMean(obs)

    assert(mean > 1.99999 && mean < 2.00001)
  }

  test("take simple mean of inequally weighted points") {
    val obs = Seq((0, 1), (1, 12), (2, 4), (3, 2), (4, 1))

    val mean = CoverageStats.takeMean(obs)

    assert(mean > 1.49999 && mean < 1.50001)
  }

  test("take simple median of all equally weighted points") {
    val obs = Seq((0, 1), (1, 1), (2, 1), (3, 1), (4, 1))

    val mean = CoverageStats.takeMedian(obs)

    assert(mean > 1.99999 && mean < 2.00001)
  }

  test("take simple median of inequally weighted points") {
    val obs = Seq((0, 1), (1, 12), (2, 4), (3, 2), (4, 1))

    val mean = CoverageStats.takeMedian(obs)

    assert(mean > 0.99999 && mean < 1.00001)
  }

  test("take median when it lies between points") {
    val obs = Seq((0, 1), (1, 1))

    val mean = CoverageStats.takeMedian(obs)

    assert(mean > 0.49999 && mean < 0.50001)
  }

  val cs = {
    val obs = Map((0 -> 1), (1 -> 1), (2 -> 3))

    CoverageStats(2.0, 7.0 / 5.0, obs)
  }

  test("covered at 0 should return all sites") {
    val sites = cs.sitesCoveredAt(0)
    val percentSites = cs.percentSitesCoveredAt(0)

    assert(sites === 5)
    assert(percentSites > 99.999 && percentSites < 100.001)
  }

  test("covered at 2 should return fewer sites") {
    val sites = cs.sitesCoveredAt(2)
    val percentSites = cs.percentSitesCoveredAt(2)

    assert(sites === 3)
    assert(percentSites > 59.999 && percentSites < 60.001)
  }

  test("covered at 5 should return no sites") {
    val sites = cs.sitesCoveredAt(5)
    val percentSites = cs.percentSitesCoveredAt(5)

    assert(sites === 0)
    assert(percentSites === 0.0)
  }
}
