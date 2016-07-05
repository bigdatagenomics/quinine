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

private[cli] object StatWriter {

  /**
   * Helper function to print stats out as a file or to standard out.
   *
   * @param stats The stat to print.
   * @param path An optional desired path. If the path is not null, we write
   *   the stats to the file at this path. If null, we write the stats to
   *   standard out.
   * @param sc A Spark Context. Used to get the underlying file system.
   */
  def apply(stats: Any,
            path: String,
            sc: SparkContext) {
    apply(stats, Option(path), sc)
  }

  /**
   * Helper function to print stats out as a file or to standard out.
   *
   * @param stats The stat to print.
   * @param optPath An optional desired path. If a path is provided, we write
   *   the stats to the file at this path. If not provided, we write the stats
   *   to standard out.
   * @param sc A Spark Context. Used to get the underlying file system.
   */
  def apply(stats: Any,
            optPath: Option[String],
            sc: SparkContext) {
    optPath match {
      case Some(path) => {
        // get the path
        val path = optPath.get

        // get the underlying file system
        val fs = FileSystem.get(sc.hadoopConfiguration)

        // get a stream to write to a file
        val os = fs.create(new Path(path))
          .asInstanceOf[OutputStream]

        // write the stats
        os.write(stats.toString.getBytes)

        // close the output stream
        os.flush()
        os.close()
      }
      case None => println(stats)
    }
  }
}
