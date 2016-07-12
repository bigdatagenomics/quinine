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
package org.bdgenomics.quinine.rdd

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{ MultiContigNonoverlappingRegions, ReferenceRegion }
import scala.reflect.ClassTag

private[quinine] object RightOuterJoin extends Serializable {

  /**
   * Performs a right outer region join.
   *
   * @param baseRDD Right RDD.
   * @param joinedRDD Right RDD.
   * @return Returns the right outer region join of two RDDs. If no value in the
   *   left RDD overlaps with a value in the right RDD, the right RDD will be
   *   keyed with a None. If there is an overlap, we will return key-value pairs
   *   with all joined pairs.
   *
   * @tparam T The type of the value in the left RDD.
   * @tparam U The type of the value in the right RDD.
   */
  def apply[T, U](
    baseRDD: RDD[(ReferenceRegion, T)],
    joinedRDD: RDD[(ReferenceRegion, U)])(implicit tManifest: ClassTag[T],
                                          uManifest: ClassTag[U]): RDD[(Option[T], U)] = {

    val sc = baseRDD.context

    /**
     * Original Join Design:
     *
     * Parameters:
     *   (1) f : (Range, Range) => T  // an aggregation function
     *   (2) a : RDD[Range]
     *   (3) b : RDD[Range]
     *
     * Return type: RDD[(Range,T)]
     *
     * Algorithm:
     *   1. a.collect() (where a is smaller than b)
     *   2. build a non-overlapping partition on a
     *   3. ak = a.map( v => (partition(v), v) )
     *   4. bk = b.flatMap( v => partitions(v).map( i=>(i,v) ) )
     *   5. joined = ak.join(bk).filter( (i, (r1, r2)) => r1.overlaps(r2) ).map( (i, (r1,r2))=>(r1, r2) )
     *   6. return: joined.reduceByKey(f)
     *
     * Ways in which we've generalized this plan:
     * - removed the aggregation step altogether
     * - carry a sequence dictionary through the computation.
     */

    // First, we group the regions in the left side of the join by their referenceName,
    // and collect them.
    val collectedLeft: Seq[(String, Iterable[ReferenceRegion])] =
      baseRDD
        .map(_._1) // RDD[ReferenceRegion]
        .keyBy(_.referenceName) // RDD[(String,ReferenceRegion)]
        .groupByKey() // RDD[(String,Seq[ReferenceRegion])]
        .collect() // Iterable[(String,Seq[ReferenceRegion])]
        .toSeq // Seq[(String,Seq[ReferenceRegion])]

    // Next, we turn that into a data structure that reduces those regions to their non-overlapping
    // pieces, which we will use as a partition.
    val multiNonOverlapping = new MultiContigNonoverlappingRegions(collectedLeft)

    // Then, we broadcast those partitions -- this will be the function that allows us to
    // partition all the regions on the right side of the join.
    val regions = sc.broadcast(multiNonOverlapping)

    // each element of the left-side RDD should have exactly one partition.
    val smallerKeyed: RDD[(Option[ReferenceRegion], (ReferenceRegion, T))] =
      baseRDD.map(t => (Some(regions.value.regionsFor(t).head), t))

    // each element of the right-side RDD may have 0, 1, or more than 1 corresponding partition.
    val largerKeyed: RDD[(Option[ReferenceRegion], (ReferenceRegion, U))] =
      joinedRDD.flatMap(t => {
        val reg = regions.value.regionsFor(t)

        // run join
        if (reg.isEmpty) {
          Iterable((None.asInstanceOf[Option[ReferenceRegion]], t))
        } else {
          reg.map((r: ReferenceRegion) => (Some(r), t))
        }
      })

    // this is (essentially) performing a cartesian product within each partition...
    val joined: RDD[(Option[ReferenceRegion], (Option[(ReferenceRegion, T)], (ReferenceRegion, U)))] =
      smallerKeyed.rightOuterJoin(largerKeyed)

    // ... so we need to filter the final pairs to make sure they're overlapping.
    val filtered = joined.flatMap(kv => {
      val (_, (t: Option[(ReferenceRegion, T)], u: (ReferenceRegion, U))) = kv

      t.fold(Some((None.asInstanceOf[Option[T]], u._2)).asInstanceOf[Option[(Option[T], U)]])(p => {
        if (p._1.overlaps(u._1)) {
          Some((Some(p._2), u._2)).asInstanceOf[Option[(Option[T], U)]]
        } else {
          None.asInstanceOf[Option[(Option[T], U)]]
        }
      })
    })

    filtered
  }
}
