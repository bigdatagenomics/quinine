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
package org.bdgenomics.quinine.metrics.contamination

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.BroadcastRegionJoin
import org.bdgenomics.formats.avro.{
  AlignmentRecord,
  DatabaseVariantAnnotation,
  Genotype
}

object ContaminationEstimator {

  /**
   * Estimates inter-sample contamination given reads, prior genotypes, and
   * frequency information.
   *
   * @param reads NGS reads from a sample, to analyze for contamination.
   * @param genotypes Data from a prior genotyping study.
   * @param annotations Allele frequency annotations.
   * @return Returns the estimated contamination.
   */
  def estimateContamination(
    reads: RDD[AlignmentRecord],
    genotypes: RDD[Genotype],
    annotations: RDD[DatabaseVariantAnnotation]): ContaminationEstimate = {

    // get rdd of variant sites
    val variantSites = VariantSite(genotypes, annotations)

    // create estimator
    val estimator = ContaminationEstimator(reads, variantSites)

    // run the estimation process and return
    estimator.estimateContamination()
  }
}

/**
 * Estimates the cross-individual contamination in next-generating sequencing data,
 * using the ContEst algorithm (http://bioinformatics.oxfordjournals.org/content/27/18/2601)
 *
 * @param reads The set of reads used to estimate the contamination
 * @param variants A baseline set of variants (and their sites in the genome)
 */
private[contamination] case class ContaminationEstimator(val reads: RDD[AlignmentRecord],
                                                         val variants: RDD[VariantSite]) {

  /**
   * This is the main entry-point to the code -- it estimates a contamination parameter 'c'
   * by maximum-likelihood over a grid of possible values.
   *
   * @return The 'c' which maximizes the likelihood in the grid of possible values.
   */
  def estimateContamination(): ContaminationEstimate = {

    val observations = BroadcastRegionJoin.partitionAndJoin(variants.keyBy(_.getRegion),
      reads.filter(_.getReadMapped)
        .keyBy(ReferenceRegion(_)))
      .flatMap(kv => {
        val (variantSite, read) = kv
        variantSite.toObservation(read)
      }).cache()

    // count the number of observations and throw an exception if empty
    // this will also force the rdd into the cache
    require(observations.count > 0, "%s\n%s\n%s\n%s".format(
      "Didn't observe any variants that overlapped with reads. Possible causes:",
      "1. No HomAlt sites in input VCF,",
      "2. Reads and variants are on different reference builds,",
      "3. No HomAlt sites had allele frequency annotations."))

    // build the grid to search over
    val grid = ((1 until 10).map(i => 0.001 * i) ++
      (1 until 10).map(i => 0.01 * i) ++
      (1 until 10).map(i => 0.1 * i))

    // loop over the grid and get the complete log likelihoods
    val lls = grid.map(logLikelihood(observations, _))

    // return an estimate instance
    ContaminationEstimate(grid, lls)
  }

  /**
   * Calculates the log-likelihood of a particular contamination level, given the set of reads
   * and the set of variant sites.
   *
   * @param observations An RDD containing variant sites observations against reads.
   * @param contamination The contamination level over which to calculate the log-likelihood
   * @return The log-likelihood
   */
  def logLikelihood(observations: RDD[Observation],
                    contamination: Double): Double = {

    observations.map(_.logLikelihood(contamination))
      .reduce(_ + _)
  }
}
