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
import org.bdgenomics.quinine.metrics.contamination.ContaminationEstimator
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }

object EstimateContamination extends BDGCommandCompanion {

  val commandName = "estimateContamination"
  val commandDescription = "Estimates the cross-sample contamination in a set of reads."

  def apply(cmdLine: Array[String]): BDGCommand = {
    new EstimateContamination(Args4j[EstimateContaminationArgs](cmdLine))
  }
}

class EstimateContaminationArgs extends Args4jBase {
  @Argument(required = true,
    metaVar = "READS",
    usage = "The sequenced dataset.",
    index = 0)
  val readPath: String = null

  @Argument(required = true,
    metaVar = "GENOTYPES",
    usage = "The genotypes from a prior genotyping study.",
    index = 1)
  val genotypePath: String = null

  @Argument(required = true,
    metaVar = "ANNOTATIONS",
    usage = "Variant annotations with minor allele frequencies.",
    index = 2)
  val annotationPath: String = null

  @Args4jOption(required = false,
    name = "-statPath",
    usage = "File to write stats to. If omitted, writes to standard out.")
  val statPath: String = null
}

class EstimateContamination(val args: EstimateContaminationArgs) extends BDGSparkCommand[EstimateContaminationArgs] {

  val companion = EstimateContamination

  def run(sc: SparkContext) {

    // load in the reads
    val reads = sc.loadAlignments(args.readPath)

    // load in the reads
    val genotypes = sc.loadGenotypes(args.genotypePath)

    // load in the reads
    val annotations = sc.loadVariantAnnotations(args.annotationPath)

    // compute the contamination
    val contaminationStats = ContaminationEstimator.estimateContamination(reads,
      genotypes,
      annotations)

    // write out the stats
    StatWriter(contaminationStats, args.statPath, sc)
  }
}
