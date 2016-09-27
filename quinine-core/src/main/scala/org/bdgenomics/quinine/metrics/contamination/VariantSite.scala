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
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.util.PhredUtils
import org.bdgenomics.formats.avro.{
  AlignmentRecord,
  DatabaseVariantAnnotation,
  Genotype,
  GenotypeAllele,
  Variant
}
import scala.collection.JavaConversions._

private[contamination] object VariantSite extends Serializable {

  /**
   * Populates variant site RDD.
   *
   * The variant site RDD is populated by taking all homozygous alternate sites
   * from a genotyping study, and joining against previously estimated minor
   * allele frequencies.
   *
   * @param genotypes RDD to find homozygous alt sites from a prior genotyping
   *   study.
   * @param annotations An RDD of annotations containing allele frequencies.
   * @return Returns an RDD of variant sites.
   */
  def apply(genotypes: RDD[Genotype],
            annotations: RDD[DatabaseVariantAnnotation]): RDD[VariantSite] = {

    // filter to find homozygous alt genotypes
    // and make an rdd keyed by the variant
    val homAltGenotypes = genotypes.filter(g => {
      g.getAlleles.forall(_ == GenotypeAllele.Alt)
    }).map(g => {
      val v = g.getVariant
      ((v.getContig.getContigName,
        v.getStart,
        v.getEnd,
        v.getReferenceAllele,
        v.getAlternateAllele), null)
    })

    // map annotations to allele frequency and key by variant
    val frequencies = annotations.map(a => {
      val v = a.getVariant
      ((v.getContig.getContigName,
        v.getStart,
        v.getEnd,
        v.getReferenceAllele,
        v.getAlternateAllele), (v, a.getThousandGenomesAlleleFrequency))
    })

    // do join and emit sites
    homAltGenotypes.join(frequencies)
      .map(kv => {
        val (_, (_, (variant, frequency))) = kv

        VariantSite(variant, frequency.toDouble)
      })
  }
}

/**
 * In the ContEST paper, the variable 'i' indexes the sites of know variation, where
 * A_i is the reference allele (and \bar{A}_i the non-reference allele) at that site, and
 * f_i is the alternative (contaminating) population frequency of the variant.
 *
 * This class, VariantSite, encapsulates both the A_i and the f_i for a particular
 * variant site i.
 *
 * @param variant The variant with-respect-to-which we are calculating the likelihood of any read.
 * @param populationAlternateFrequency a frequency, specific to this Variant
 */
private[contamination] case class VariantSite(variant: Variant,
                                              minorAlleleFrequency: Double) extends Logging {

  /**
   * @return Returns the reference region that this variant site spans.
   */
  def getRegion: ReferenceRegion = {
    ReferenceRegion(variant.getContig.getContigName, variant.getStart, variant.getEnd)
  }

  /**
   * Turns a variant site into an observation of that variant site.
   * The observation model then implements our likelihood model.
   *
   * @param read The read whose likelihood we are assessing.
   * @return If the read matches the reference or alternate allele, an
   *   instance of an observation, where the base quality is used to set the
   *   error probability, and the minor allele frequency is passed.
   */
  def toObservation(read: AlignmentRecord): Option[Observation] = {
    try {
      val offset = (variant.getStart - read.getStart).toInt
      val readBase = read.getSequence.charAt(offset).toString
      val baseQuality = read.getQual.charAt(offset).toInt - 33
      val error = PhredUtils.phredToErrorProbability(baseQuality)

      if (readBase == variant.getReferenceAllele) {
        Some(ReferenceObservation(error, minorAlleleFrequency))
      } else if (readBase == variant.getAlternateAllele) {
        Some(AlternateObservation(error, minorAlleleFrequency))
      } else {
        None
      }
    } catch {
      case t: Throwable => {
        log.warn("Failed to create observation for read %s due to %s. Dropping...".format(
          read.getReadName,
          t))
        None
      }
    }
  }
}
