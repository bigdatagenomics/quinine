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

import htsjdk.variant.variantcontext.VariantContext
import htsjdk.variant.vcf.VCFConstants
import org.apache.hadoop.io.LongWritable
import org.apache.parquet.hadoop.util.ContextUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.utils.misc.HadoopUtil
import org.bdgenomics.formats.avro.{ Contig, DatabaseVariantAnnotation, Variant }
import org.seqdoop.hadoop_bam._

private[quinine] object AFFromVCF extends Serializable {

  def apply(filePath: String,
            sc: SparkContext): RDD[DatabaseVariantAnnotation] = {
    loadVcs(filePath, sc).flatMap(vcwToOptionalAnnotation)
  }

  private def loadVcs(filePath: String,
                      sc: SparkContext): RDD[VariantContextWritable] = {
    val job = HadoopUtil.newJob(sc)
    sc.newAPIHadoopFile(
      filePath,
      classOf[VCFInputFormat], classOf[LongWritable], classOf[VariantContextWritable],
      ContextUtil.getConfiguration(job)).map(_._2)
  }

  private def vcwToOptionalAnnotation(vcw: VariantContextWritable): Option[DatabaseVariantAnnotation] = {
    vcToOptionalAnnotation(vcw.get)
  }

  private def vcToOptionalAnnotation(vc: VariantContext): Option[DatabaseVariantAnnotation] = {
    if (vc.isBiallelic && vc.hasAttribute("AF")) {
      val ctg = Contig.newBuilder()
        .setContigName(vc.getContig())
        .build()
      val v = Variant.newBuilder()
        .setContig(ctg)
        .setStart(vc.getStart.toLong - 1) // 1 indexed
        .setEnd(vc.getEnd.toLong) // closed intervals
        .setReferenceAllele(vc.getReference.getBaseString)
        .setAlternateAllele(vc.getAlternateAllele(0).getBaseString)
        .build()

      val af: java.lang.Float = vc.getAttribute(VCFConstants.ALLELE_FREQUENCY_KEY) match {
        case f: java.lang.Float => f
        case d: java.lang.Double => {
          val scalaD: Double = d
          d.toFloat
        }
        case s: String => {
          try {
            new java.lang.Float(s)
          } catch {
            case e: Throwable => {
              throw new IllegalStateException("Received String AF value from %s, but got exception %s converting to float.".format(
                vc.toString, e))
            }
          }
        }
        case _ => throw new IllegalStateException("Illegal AF value type for %s.".format(
          vc.toString))
      }

      Some(DatabaseVariantAnnotation.newBuilder()
        .setVariant(v)
        .setThousandGenomesAlleleFrequency(af)
        .build())
    } else {
      None
    }
  }
}
