package sgc.cuboid

import spark.RDD

case class CuboidEntry(fun : AggregateFunction, size : Long, cuboid : RDD[Pair[String,Long]])
