package sgc.materialization

import sgc.cuboid.{CuboidQuery, AggregateFunction, CuboidEntry}
import spark.storage.StorageLevel


object MinLevelStrategy {

  def materialize(maxCuboids : Int, minLevel : Int, numberOfDimensions : Int, baseCuboid : CuboidEntry) : GraphCube = {

    val graphCube = new GraphCube(numberOfDimensions,minLevel,baseCuboid)

    def generateCuboid(count : Int, aggregateLevel : Int, combinations : Seq[Seq[Int]]) : Unit = {

      if (count == maxCuboids) return
      combinations match{
        case Nil if aggregateLevel != 1 =>  {
          generateCuboid(count, aggregateLevel - 1, CombinationsGenerator.comb(aggregateLevel,numberOfDimensions))
        }
        case head :: tail => {
          val fun = new AggregateFunction(head)
          val cuboid = CuboidQuery.query(baseCuboid.cuboid, fun, numberOfDimensions)
          cuboid.persist(StorageLevel.MEMORY_ONLY)
          graphCube.addCuboid(CuboidEntry(fun, cuboid.count(),cuboid))
          generateCuboid(count + 1, aggregateLevel, tail)
        }
        case _ => return
      }
    }

    generateCuboid(0,numberOfDimensions - minLevel + 1, Nil)

    graphCube


  }

}
