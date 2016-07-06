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

import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.{ Logging, SparkContext }
import org.bdgenomics.adam.projections.GenotypeField
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.Genotype
import org.bdgenomics.quinine.rdd.QCContext._
import org.bdgenomics.quinine.rdd.variation.ConcordanceTable
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }

object GenotypeConcordance extends BDGCommandCompanion {
  val commandName = "genotype_concordance"
  val commandDescription = "Pairwise comparison of sets of ADAM genotypes"

  def apply(cmdLine: Array[String]) = {
    new GenotypeConcordance(Args4j[GenotypeConcordanceArgs](cmdLine))
  }
}

class GenotypeConcordanceArgs extends Args4jBase with ParquetArgs {
  @Argument(required = true, metaVar = "test", usage = "The test ADAM genotypes file", index = 0)
  var testGenotypesFile: String = _
  @Argument(required = true, metaVar = "truth", usage = "The truth ADAM genotypes file", index = 1)
  var truthGenotypesFile: String = _
  @Args4jOption(required = false, name = "-include_non_pass", usage = "Include non-PASSing sites in concordance evaluation")
  var includeNonPass: Boolean = false
}

class GenotypeConcordance(protected val args: GenotypeConcordanceArgs) extends BDGSparkCommand[GenotypeConcordanceArgs] with Logging {
  val companion: BDGCommandCompanion = GenotypeConcordance

  def run(sc: SparkContext): Unit = {
    // TODO: Figure out projections of nested fields
    var project = List(
      GenotypeField.variant, GenotypeField.sampleId, GenotypeField.alleles)

    def filterOnPass(gt: Genotype): Boolean = {
      Option(gt.getVariantCallingAnnotations).fold(false)(vca => {
        Option(vca.getVariantIsPassing).fold(false)(v => v)
      })
    }

    val testGTs: RDD[Genotype] = sc.loadGenotypes(args.testGenotypesFile)
      .filter(filterOnPass)
    val truthGTs: RDD[Genotype] = sc.loadGenotypes(args.truthGenotypesFile)
      .filter(filterOnPass)

    val tables = testGTs.concordanceWith(truthGTs)

    // Write out results as a table
    System.out.println("Sample\tConcordance\tNonReferenceSensitivity")
    for ((sample, table) <- tables.collectAsMap()) {
      System.out.println("%s\t%f\t%f".format(sample, table.concordance, table.nonReferenceSensitivity))
    }
    {
      val total = tables.values.fold(ConcordanceTable())((c1, c2) => c1.add(c2))
      System.out.println("ALL\t%f\t%f".format(total.concordance, total.nonReferenceSensitivity))
    }

  }
}
