package sgc.cuboid

import spark.RDD

case class CuboidEntry(fun : Cuboid, size : Long, cuboid : RDD[Pair[String,Long]])
