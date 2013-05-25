package sgc.cuboid

import spark.RDD
import spark.SparkContext._
import sgc.graph.VertexIDParser



object CuboidQuery {

  def cuboidQuery(rdd : RDD[Pair[String,Long]], cub : Cuboid) : RDD[Pair[String,Long]] = {
    val inter = rdd.map( entry => Pair(cub.aggregate(VertexIDParser.parseID(entry._1)), entry._2))
    inter.reduceByKey(_ + _)
  }

  def crossboidQuery(rdd : RDD[Pair[String,Long]], cub1 : Cuboid, cub2 : Cuboid
                        ) : RDD[Pair[String,Long]] = {
    val inter = rdd.flatMap( entry => {
      val aggregatedElems =  Cuboid.crossAggregate(cub1,cub2, VertexIDParser.parseID(entry._1))
      Seq(Pair(aggregatedElems._1, entry._2), Pair(aggregatedElems._2, entry._2))
    })
    inter.reduceByKey(_ + _)
  }
}
