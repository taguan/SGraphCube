package sgc.cuboid

import spark.RDD
import spark.SparkContext._
import sgc.graph.{VertexIDParser, ArrayVertexID}


object CuboidQuery {

  def query(rdd : RDD[Pair[String,Long]], fun : AggregateFunction, numberOfDimensions : Int ) : RDD[Pair[String,Long]] = {
    rdd.map( entry => fun.aggregateVertex(VertexIDParser.parseID(entry_1)))
  }
}
