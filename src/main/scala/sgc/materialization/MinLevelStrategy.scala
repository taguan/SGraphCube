package sgc.materialization

import sgc.cuboid.CuboidEntry


object MinLevelStrategy {

  def materialize(maxCuboids : Int, minLevel : Int, numberOfDimensions : Int, baseCuboid : CuboidEntry) : GraphCube = {
    val graphCube = new GraphCube(numberOfDimensions,minLevel,baseCuboid)
    graphCube

  }

}
