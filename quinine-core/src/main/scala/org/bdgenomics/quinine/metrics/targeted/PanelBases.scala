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

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferencePosition
import org.bdgenomics.formats.avro.Feature

/**
 * Helper object for computing panel base coverage statistics.
 */
private[targeted] object PanelBases {

  /**
   * Given an RDD of targets and bait, computes a set of statistics.
   *
   * Computes the number of bases that are uniquely covered by bait, the number
   * of bases covered by targets, and the number of uniquely covered bait bases
   * that do not map to a target. By "uniquely covered," we mean that we only
   * count a base that is covered by multiple bait once.
   *
   * @param targets An RDD with targeted regions.
   * @param bait An RDD with the baited regions.
   * @return Returns statistics.
   */
  def apply(targets: RDD[Feature],
            bait: RDD[Feature]): PanelBases = {

    def targetsToRegions(rdd: RDD[Feature]): RDD[(ReferencePosition, Null)] = {
      rdd.flatMap(f => {
        (f.getStart.toInt until f.getEnd.toInt).map(pos => {
          (ReferencePosition(f.getContig.getContigName, pos.toLong), null)
        })
      })
    }

    // get all bases covered by targets
    val targetBases = targetsToRegions(targets)
      .cache()

    // get all bases covered by bait
    val baitBases = targetsToRegions(bait)
      .distinct()
      .cache()

    // do a right outer join to see whether a bait base is on a target
    val offTargetBaitBases = targetBases.rightOuterJoin(baitBases)
      .filter(kv => kv._2._1.isEmpty)

    // run our counts to generate stats
    val stats = PanelBases(targetBases.count,
      baitBases.count,
      offTargetBaitBases.count)

    // unpersist rdds
    targetBases.unpersist()
    baitBases.unpersist()

    stats
  }
}

/**
 * Statistics about bait/target coverage from a panel.
 *
 * @param targetBases The number of bases targeted by the sequencing protocol.
 * @param uniqueBaitBases The number of bases covered by one or more bait.
 * @param offTargetBaitBases The number of bases covered by one or more bait
 *   that do _not_ cover a targeted base.
 */
private[targeted] case class PanelBases(targetBases: Long,
                                        uniqueBaitBases: Long,
                                        offTargetBaitBases: Long) {

  /**
   * @return The percentage of baited bases that are not on a targeted base.
   */
  def percentOffTarget: Double = {
    100.0 * offTargetBaitBases.toDouble / uniqueBaitBases.toDouble
  }

  /**
   * @return The fraction of baited bases that cover a targeted base.
   */
  def panelEfficiency: Double = {
    targetBases.toDouble / uniqueBaitBases.toDouble
  }
}
