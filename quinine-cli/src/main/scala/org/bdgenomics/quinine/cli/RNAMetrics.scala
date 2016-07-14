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
import org.bdgenomics.quinine.metrics.rna.RNASeqPanelStats
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }

object RNAMetrics extends BDGCommandCompanion {

  val commandName = "rnaMetrics"
  val commandDescription = "Computes stats for RNA-seq reads mapped to a reference transcriptome."

  def apply(cmdLine: Array[String]): BDGCommand = {
    new RNAMetrics(Args4j[RNAMetricsArgs](cmdLine))
  }
}

class RNAMetricsArgs extends Args4jBase {
  @Argument(required = true,
    metaVar = "READS",
    usage = "The sequenced RNA-seq dataset.",
    index = 0)
  var readPath: String = null

  @Argument(required = true,
    metaVar = "TRANSCRIPTS",
    usage = "The descriptions of the transcripts.",
    index = 1)
  var featuresPath: String = null

  @Args4jOption(required = false,
    name = "-startEndLength",
    usage = "The width of the region to consider as the start/end of a transcript. Default is 100bp.")
  var startEndWidth: Int = 100

  @Args4jOption(required = false,
    name = "-statPath",
    usage = "File to write stats to. If omitted, writes to standard out.")
  var statPath: String = null
}

class RNAMetrics(val args: RNAMetricsArgs) extends BDGSparkCommand[RNAMetricsArgs] {

  val companion = RNAMetrics

  def run(sc: SparkContext) {

    // load in the reads
    val reads = sc.loadAlignments(args.readPath)

    // load in the features
    val transcripts = sc.loadFeatures(args.featuresPath)

    // compute the stats
    val stats = RNASeqPanelStats(reads, transcripts, args.startEndWidth)

    // write the stats out
    StatWriter(stats, args.statPath, sc)
  }
}
