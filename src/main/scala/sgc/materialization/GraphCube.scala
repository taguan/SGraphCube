package sgc.materialization

import java.util
import sgc.cuboid.{CuboidQuery, AggregateFunction, CuboidEntry}
import spark.{Logging, RDD}
import spark.storage.StorageLevel

class GraphCube(numberOfDimensions : Int, minLevel : Int, baseCuboid : CuboidEntry) extends Logging {

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
   * Gets the nearest common descendant  stored in the graphcube
   */
  def getNearestDescendant(funs : AggregateFunction*) : CuboidEntry = {
    def getMaxLevel(index : Int, max : Int) : Int = {
      if (index == funs.length) return max
      if (getLevel(funs(index)) > max) getMaxLevel(index + 1, getLevel(funs(index)))
      else getMaxLevel(index + 1, max)
    }

    def allDescendants(index : Int, fun : AggregateFunction) : Boolean = {
      if (index == funs.length) return true
      if (!fun.isDescendant(funs(index))) return false
      allDescendants(index + 1, fun)
    }

    var level = getMaxLevel(0, -1)

    if(level == numberOfDimensions){ // base cuboid
      return getBaseCuboid
    }

    while(level < numberOfDimensions){

      val descendantLevel = graphCube.get(level)

      for(i <- 0 until descendantLevel.size()){
        val cuboid = descendantLevel.get(i)
        if(allDescendants(0,cuboid.fun)){
          return cuboid
        }
      }
      level = level + 1
    }

    getBaseCuboid
  }


  /**
   * Gets the materialized cuboid representing func if presents
   * in the GraphCube, NONE otherwise
   * @param func  AggregateFunction representing the searched cuboid
   * @return  The desired CuboidEntry, None if this cuboid is not in the GraphCube
   */
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

  /**
   * Updates the rdd of an entry with given func
   * @param func  AggregateFunction representing the CuboidEntry to be modified
   * @param newRDD   The newRDD (typically the same as previously with another StorageLevel)
   * @return   True if a CuboidEntry has been updated, false otherwise
   */
  def modifyEntry(func : AggregateFunction, newRDD : RDD[Pair[String,Long]]) = {
    val cuboidLevel = graphCube.get(getLevel(func))

    for(i <- 0 until cuboidLevel.size()){
      val cuboid = cuboidLevel.get(i)
      if(cuboid.fun.equals(func)){
        val newEntry = CuboidEntry(func, cuboid.size, newRDD)
        cuboidLevel.set(i, newEntry)
        true
      }
    }
    false
  }
  /**
   * Generate a new cuboid corresponding to func according to Graph Cube techniques
   * if not already present in the GraphCube
   * If already materialized, changes the persistence level to MEMORY_AND_DISK if it
   * was stored on DISK_ONLY
   * @param fun The AggregateFunction of the desired cuboid
   * @return  The cuboid on its RDD form
   */
  def generateOrGetCuboid(fun : AggregateFunction) : RDD[Pair[String,Long]] = {
    this.get(fun) match {
      case Some(cuboidEntry) => {
        logInfo("Cuboid " + fun + " has been found in the graph cube" )
        if (cuboidEntry.cuboid.getStorageLevel == StorageLevel.DISK_ONLY){  //it was a cuboid from materialization step
          val requestedGraph = cuboidEntry.cuboid.map(entry => entry)  //this is useless but we have to create
          //a new RDD as Spark cannot change persistence level
          requestedGraph.persist(StorageLevel.MEMORY_AND_DISK) //we dont want to have to recompute the materialized cuboid
          //if it evicted from memory
          logInfo("Cuboid on disk marked to be loaded in memory (delayed action !)")
          this.modifyEntry(fun,requestedGraph)  //next time this entry will be used, the in memory version will be selected

          requestedGraph
        }
        else{
          cuboidEntry.cuboid
        }
      }
      case None => {
        //apply the graphcube method to compute the cuboid
        val descendant = this.getNearestDescendant(fun)
        logInfo("Descendant found : " + descendant.fun + " of size " + descendant.size)
        val requestedGraph = CuboidQuery.generateCuboid(descendant.cuboid, fun, numberOfDimensions)
        requestedGraph.persist(StorageLevel.MEMORY_ONLY)
        val cuboidSize = requestedGraph.count()
        logInfo("Size of new cuboid : " + cuboidSize)
        this.addCuboid(CuboidEntry(fun,cuboidSize,requestedGraph))

        requestedGraph
      }
    }

  }

  def getLevel(func : AggregateFunction) = {
    numberOfDimensions - func.dimToAggregate.size
  }

  def getBaseCuboid : CuboidEntry = {
    graphCube.get(numberOfDimensions).get(0)
  }

}
