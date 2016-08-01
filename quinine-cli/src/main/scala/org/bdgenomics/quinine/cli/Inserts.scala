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
import org.bdgenomics.quinine.metrics.insert.InsertSizeDistribution
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }

object Inserts extends BDGCommandCompanion {

  val commandName = "insertSizeDistribution"
  val commandDescription = "Computes the insert size distribution of a set of reads."

  def apply(cmdLine: Array[String]): BDGCommand = {
    new Inserts(Args4j[InsertsArgs](cmdLine))
  }
}

class InsertsArgs extends Args4jBase {
  @Argument(required = true,
    metaVar = "READS",
    usage = "The sequenced dataset.",
    index = 0)
  val readPath: String = null

  @Args4jOption(required = false,
    name = "-statPath",
    usage = "File to write stats to. If omitted, writes to standard out.")
  val statPath: String = null
}

class Inserts(val args: InsertsArgs) extends BDGSparkCommand[InsertsArgs] {

  val companion = Inserts

  def run(sc: SparkContext) {

    // load in the reads
    val reads = sc.loadAlignments(args.readPath)

    // compute the insert size distribution
    val insertSizeDistribution = InsertSizeDistribution(reads)

    // write out the stats
    StatWriter(insertSizeDistribution, args.statPath, sc)
  }
}
