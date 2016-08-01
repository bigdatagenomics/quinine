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
import org.bdgenomics.quinine.annotations.AFFromVCF
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.Argument

object LoadAlleleFrequency extends BDGCommandCompanion {

  val commandName = "loadAlleleFrequency"
  val commandDescription = "Loads minor allele frequencies across called genotypes."

  def apply(cmdLine: Array[String]): BDGCommand = {
    new LoadAlleleFrequency(Args4j[LoadAlleleFrequencyArgs](cmdLine))
  }
}

class LoadAlleleFrequencyArgs extends Args4jBase {
  @Argument(required = true,
    metaVar = "VCF",
    usage = "The VCF to load allele frequencies from.",
    index = 0)
  val vcfPath: String = null

  @Argument(required = true,
    metaVar = "ANNOTATIONS",
    usage = "Where to save variant annotations with minor allele frequencies.",
    index = 1)
  val outputPath: String = null
}

class LoadAlleleFrequency(val args: LoadAlleleFrequencyArgs) extends BDGSparkCommand[LoadAlleleFrequencyArgs] {

  val companion = LoadAlleleFrequency

  def run(sc: SparkContext) {

    // compute the allele frequencies
    val annotations = AFFromVCF(args.vcfPath, sc)

    // save the allele frequencies
    annotations.adamParquetSave(args.outputPath)
  }
}
