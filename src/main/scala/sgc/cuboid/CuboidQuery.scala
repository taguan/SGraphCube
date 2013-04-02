package sgc.cuboid

import spark.RDD
import spark.SparkContext._
import sgc.graph.VertexIDParser



object CuboidQuery {

  def generateCuboid(rdd : RDD[Pair[String,Long]], fun : AggregateFunction, numberOfDimensions : Int ) : RDD[Pair[String,Long]] = {
    val inter = rdd.map( entry => Pair(fun.aggregate(VertexIDParser.parseID(entry._1)), entry._2))
    inter.reduceByKey(_ + _)
  }

  def generateCrossboid(rdd : RDD[Pair[String,Long]], fun1 : AggregateFunction, fun2 : AggregateFunction,
                        numberOfDimensions : Int ) : RDD[Pair[String,Long]] = {
    val inter = rdd.flatMap( entry => {
      val aggregatedElems =  AggregateFunction.crossAggregate(fun1,fun2, VertexIDParser.parseID(entry._1))
      Seq(Pair(aggregatedElems._1, entry._2), Pair(aggregatedElems._2, entry._2))
    })
    inter.reduceByKey(_ + _)
  }
}
