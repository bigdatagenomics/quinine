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

import java.io.OutputStream
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.quinine.metrics.coverage.{ CoverageStats, ReadCoverage }
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }

object Coverage extends BDGCommandCompanion {

  val commandName = "coverage"
  val commandDescription = "Compute the read coverage across our panel."

  def apply(cmdLine: Array[String]): BDGCommand = {
    new Coverage(Args4j[CoverageArgs](cmdLine))
  }
}

class CoverageArgs extends Args4jBase {
  @Argument(required = true,
    metaVar = "READS",
    usage = "The sequenced dataset.",
    index = 0)
  val readPath: String = null

  @Args4jOption(required = false,
    name = "-statPath",
    usage = "File to write stats to. If omitted, writes to standard out.")
  val statPath: String = null

  @Args4jOption(required = false,
    name = "-rawCoveragePath",
    usage = "Optional path to save the raw coverage counts.")
  val coveragePath: String = null

  @Args4jOption(required = false,
    name = "-sortCoverage",
    usage = "If saving coverage, sort by position. Default is false.")
  val sortCoverage: Boolean = false

  @Args4jOption(required = false,
    name = "-saveRawCoverageAsText",
    usage = "If saving raw coverage, save as text. Default is false.")
  val saveAsText: Boolean = false
}

class Coverage(protected val args: CoverageArgs) extends BDGSparkCommand[CoverageArgs] {

  val companion = Coverage

  def run(sc: SparkContext) {

    // load in the reads
    val reads = sc.loadAlignments(args.readPath)

    // compute the intermediate coverage results
    val coverageCounts = ReadCoverage(reads)

    // do we need to save the coverage output?
    if (args.coveragePath != null) {

      // cache the coverage counts
      coverageCounts.cache()

      // need to map the keys into ref regions
      // ref pos is not a case class, ref reg is
      val coverageRdd = coverageCounts.map(kv => {
        (ReferenceRegion(kv._1), kv._2)
      })

      // convert to a dataframe
      val sqlContext = new SQLContext(sc)
      val coverageDf = sqlContext.createDataFrame(coverageRdd)

      // does this need to be sorted?
      val dfToSave = if (args.sortCoverage) {
        coverageDf.orderBy(coverageDf("_1.referenceName"),
          coverageDf("_1.start"))
      } else {
        coverageDf
      }

      // are we saving as text or not?
      if (args.saveAsText) {
        dfToSave.write.json(args.coveragePath)
      } else {
        dfToSave.write.parquet(args.coveragePath)
      }
    }

    // compute the final stats
    val coverageStats = CoverageStats(coverageCounts)

    // if desired, write the coverage stats to a file
    if (args.statPath != null) {

      // get the underlying file system
      val fs = FileSystem.get(sc.hadoopConfiguration)

      // get a stream to write to a file
      val os = fs.create(new Path(args.statPath))
        .asInstanceOf[OutputStream]

      // write the stats
      os.write(coverageStats.toString.getBytes)

      // close the output stream
      os.flush()
      os.close()
    } else {
      println(coverageStats)
    }
  }
}
