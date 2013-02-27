package sgc.materialization

import java.util
import sgc.cuboid.{AggregateFunction, CuboidEntry}

class GraphCube(numberOfDimensions : Int, minLevel : Int, baseCuboid : CuboidEntry) {

  //INIT BLOCK
  val graphCube = new util.ArrayList[util.ArrayList[CuboidEntry]](numberOfDimensions + 1)

  for (_ <- 0 to numberOfDimensions){
    graphCube.add(new util.ArrayList[CuboidEntry]())
  }

  graphCube.get(numberOfDimensions).add(baseCuboid)
  //END INIT

  /**
   * Adds the cuboid to the graphcube, at its corresponding level.
   * At the same level, cuboids are sorted by ascending size
   *
   * @param cuboid  cuboid to be added to the graphcube
   */
  def addCuboid(cuboid : CuboidEntry) {
    val currentLevel = graphCube.get(getLevel(cuboid.fun))

    if (currentLevel.isEmpty){
      currentLevel.add(cuboid)
    }
    else {
      var i = 0
      var current = currentLevel.get(i)

      while(current.size < cuboid.size){
        i = i + 1
        if(i == currentLevel.size()){
          currentLevel.add(cuboid)
          return
        }
        current = currentLevel.get(i)
      }
      currentLevel.add(i,cuboid)
    }

  }

  /**
   * Gets the nearest descendant cuboid stored in the graphcube.
   * Use MinLevel properties to speed up the search
   */
  def getNearestDescendant(func : AggregateFunction) : CuboidEntry = {
      var level = getLevel(func)

      if(level == numberOfDimensions){ // base cuboid
        return getBaseCuboid
      }

      level = level + 1 //closest descendant level
      while(level <= minLevel){

        val descendantLevel = graphCube.get(level)

        for(i <- 0 until descendantLevel.size()){
          val cuboid = descendantLevel.get(i)
          if(cuboid.fun.isDescendant(func)){
            return cuboid
          }
        }
        level = level + 1
      }

      //property of MinLevel, if no descendant found in the minLevel descendants, then the nearest
      //descendant must be the base cuboid
    graphCube.get(numberOfDimensions).get(0)

  }

  def get(func : AggregateFunction) : Option[CuboidEntry] = {
    val cuboidLevel = graphCube.get(getLevel(func))

    for(i <- 0 until cuboidLevel.size()){
      val cuboid = cuboidLevel.get(i)
      if(cuboid.fun.equals(func)){
        return Some(cuboid)
      }
    }
    None
  }


  def getLevel(func : AggregateFunction) = {
    numberOfDimensions - func.dimToAggregate.size
  }

  def getBaseCuboid : CuboidEntry = {
    graphCube.get(numberOfDimensions).get(0)
  }

}
