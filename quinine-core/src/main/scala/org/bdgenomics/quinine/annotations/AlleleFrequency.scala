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
package org.bdgenomics.quinine.annotations

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.bdgenomics.formats.avro.{
  DatabaseVariantAnnotation,
  Genotype,
  GenotypeAllele
}
import scala.collection.JavaConversions._

private[quinine] object AlleleFrequency extends Serializable {

  /**
   * Computes the minor allele frequency of variants across an RDD.
   *
   * @param rdd The RDD of genotypes to compute the minor allele frequency
   *   across.
   * @return Returns an RDD of DatabaseVariantAnnotations where the minor allele
   *   frequency has been written into the `thousandGenomesAlleleFrequency`
   *   field.
   */
  def computeMinorAlleleFrequency(rdd: RDD[Genotype]): RDD[DatabaseVariantAnnotation] = {

    // merges two tuples of ints by adding
    def aggFn(a: (Int, Int), b: (Int, Int)): (Int, Int) = {
      (a._1 + b._1, a._2 + b._2)
    }

    rdd.map(g => {

      // extract the number of genotype calls
      val alleles = g.getAlleles.size

      // and, extract the number of calls that were explicit alt calls
      val alternateAlleles = g.getAlleles.count(_ == GenotypeAllele.Alt)

      // return as a tuple, and key by the variant
      (g.getVariant, (alternateAlleles, alleles))
    }).reduceByKey(aggFn)
      .map(kv => {

        // unpack the tuple
        val (variant, (alternateAlleles, alleles)) = kv

        // build a new variant annotation with the allele frequency
        DatabaseVariantAnnotation.newBuilder()
          .setVariant(variant)
          .setThousandGenomesAlleleFrequency(alternateAlleles.toFloat /
            alleles.toFloat)
          .build()
      })
  }
}
