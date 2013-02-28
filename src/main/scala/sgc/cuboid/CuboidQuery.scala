package sgc.cuboid

import spark.RDD
import spark.SparkContext._
import sgc.graph.VertexIDParser


object CuboidQuery {

  def query(rdd : RDD[Pair[String,Long]], fun : AggregateFunction, numberOfDimensions : Int ) : RDD[Pair[String,Long]] = {
    val inter = rdd.map( entry => Pair(fun.aggregate(VertexIDParser.parseID(entry._1)), entry._2))
    inter.reduceByKey(_ + _)
  }
}
