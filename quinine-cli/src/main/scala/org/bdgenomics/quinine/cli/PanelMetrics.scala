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
package org.bdgenomics.quinine.cli

import org.apache.spark.SparkContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.quinine.metrics.targeted.PanelStats
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }

object PanelMetrics extends BDGCommandCompanion {

  val commandName = "panelMetrics"
  val commandDescription = "Compute a set of metrics for reads sequenced with a targeted sequencing panel"

  def apply(cmdLine: Array[String]): BDGCommand = {
    new PanelMetrics(Args4j[PanelMetricsArgs](cmdLine))
  }
}

class PanelMetricsArgs extends Args4jBase {
  @Argument(required = true,
    metaVar = "READS",
    usage = "The sequenced dataset.",
    index = 0)
  val readPath: String = null

  @Argument(required = true,
    metaVar = "TARGETS",
    usage = "The set of genomic regions we are targetting.",
    index = 1)
  val targetPath: String = null

  @Argument(required = true,
    metaVar = "BAIT",
    usage = "The set of hybrid bait we are using.",
    index = 2)
  val baitPath: String = null

  @Args4jOption(required = false,
    name = "-panelName",
    usage = "The name of this sequencing panel. If omitted, the TARGETS filename is used.")
  val panelName: String = null

  @Args4jOption(required = false,
    name = "-cacheReads",
    usage = "Cache the input reads before the second pass. Default is true.")
  val cacheReads: Boolean = true

  @Args4jOption(required = false,
    name = "-baseQualThreshold",
    usage = "Phred-based threshold for separating bases into high/low quality. Default is 20.")
  val baseQualThreshold: Int = 20

  @Args4jOption(required = false,
    name = "-mappingQualThreshold",
    usage = "Phred-based threshold for separating alignments into well/poorly mapped. Default is 20.")
  val mappingQualThreshold: Int = 20

  @Args4jOption(required = false,
    name = "-statPath",
    usage = "File to write stats to. If omitted, writes to standard out.")
  val statPath: String = null
}

class PanelMetrics(val args: PanelMetricsArgs) extends BDGSparkCommand[PanelMetricsArgs] {

  val companion = PanelMetrics

  def run(sc: SparkContext) {

    // load in the reads
    val reads = sc.loadAlignments(args.readPath)

    // if requested, cache the reads
    if (args.cacheReads) {
      reads.cache()
    }

    // load in the targets of the sequencing panel
    val targets = sc.loadFeatures(args.targetPath)

    // load in the bait description for the sequencing panel
    val bait = sc.loadFeatures(args.baitPath)

    // compute and print the stats
    val stats = PanelStats(reads,
      targets,
      bait,
      Option(args.panelName).getOrElse(args.targetPath),
      args.baseQualThreshold,
      args.mappingQualThreshold)

    // write out the stats
    StatWriter(stats, args.statPath, sc)
  }
}
