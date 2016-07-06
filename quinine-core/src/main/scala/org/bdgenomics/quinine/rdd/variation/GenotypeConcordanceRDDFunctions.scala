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
package org.bdgenomics.quinine.rdd.variation

import org.apache.hadoop.io.LongWritable
import org.apache.spark.SparkContext._
import org.apache.spark.{ Logging, SparkContext }
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.converters.VariantContextConverter
import org.bdgenomics.adam.models.{
  ReferenceRegion,
  SequenceDictionary,
  SequenceRecord,
  VariantContext
}
import org.bdgenomics.adam.rdd.ADAMSequenceDictionaryRDDAggregator
import org.bdgenomics.adam.rich.RichVariant
import org.bdgenomics.adam.rich.RichGenotype._
import org.bdgenomics.formats.avro.{ Genotype, GenotypeType, DatabaseVariantAnnotation }
import org.bdgenomics.utils.misc.HadoopUtil
import org.seqdoop.hadoop_bam._

class GenotypeConcordanceRDDFunctions(rdd: RDD[Genotype]) extends Serializable with Logging {

  /**
   * Compute the per-sample ConcordanceTable for this genotypes vs. the supplied
   * truth dataset. Only genotypes with ploidy <= 2 will be considered.
   * @param truth Truth genotypes
   * @return PairedRDD of sample -> ConcordanceTable
   */
  def concordanceWith(truth: RDD[Genotype]): RDD[(String, ConcordanceTable)] = {
    // Concordance approach only works for ploidy <= 2, e.g. diploid/haploid
    val keyedTest = rdd.filter(_.ploidy <= 2)
      .keyBy(g => (g.getVariant, g.getSampleId.toString): (RichVariant, String))
    val keyedTruth = truth.filter(_.ploidy <= 2)
      .keyBy(g => (g.getVariant, g.getSampleId.toString): (RichVariant, String))

    val inTest = keyedTest.leftOuterJoin(keyedTruth)
    val justInTruth = keyedTruth.subtractByKey(inTest)

    // Compute RDD[sample -> ConcordanceTable] across variants/samples
    val inTestPairs = inTest.map({
      case ((_, sample), (l, Some(r))) => sample -> (l.getType, r.getType)
      case ((_, sample), (l, None))    => sample -> (l.getType, GenotypeType.NO_CALL)
    })
    val justInTruthPairs = justInTruth.map({ // "truth-only" entries
      case ((_, sample), r) => sample -> (GenotypeType.NO_CALL, r.getType)
    })

    val bySample = inTestPairs.union(justInTruthPairs).combineByKey(
      (p: (GenotypeType, GenotypeType)) => ConcordanceTable(p),
      (l: ConcordanceTable, r: (GenotypeType, GenotypeType)) => l.add(r),
      (l: ConcordanceTable, r: ConcordanceTable) => l.add(r))

    bySample
  }
}
