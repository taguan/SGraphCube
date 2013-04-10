package sgc.materialization

import org.scalatest.{BeforeAndAfter, FunSuite}
import sgc.cuboid.{AggregateFunction, CuboidEntry}


class GraphCubeSuite extends FunSuite with BeforeAndAfter {

  var graphCube : GraphCube = _

  before {
    graphCube = new GraphCube(6,3,CuboidEntry(AggregateFunction(""),1000,null))
    graphCube.addCuboid(CuboidEntry(AggregateFunction("0,2,3"),20,null))
    graphCube.addCuboid(CuboidEntry(AggregateFunction("1,2,3"),25,null))
    graphCube.addCuboid(CuboidEntry(AggregateFunction("3,4"),20,null))
    graphCube.addCuboid(CuboidEntry(AggregateFunction("1,2"),10,null))
    graphCube.addCuboid(CuboidEntry(AggregateFunction("0,1"),15,null))
  }

  /*
   * Method generateOrGetCuboid should be tested but needs to actually run Spark because
    * it uses StorageLevel
    * TODO
   */

  test("Get base cuboid"){
    assert(graphCube.getBaseCuboid.fun.equals(AggregateFunction("")))
  }

  test("Get Nearest Descendant"){
    assert(graphCube.getNearestDescendant(AggregateFunction("0,1,2,3")).fun.equals(AggregateFunction("0,2,3")))
    assert(graphCube.getNearestDescendant(AggregateFunction("1,2,3,4")).fun.equals(AggregateFunction("1,2,3")))
    assert(graphCube.getNearestDescendant(AggregateFunction("2,3,4,5")).fun.equals(AggregateFunction("3,4")))
    assert(graphCube.getNearestDescendant(AggregateFunction("1,3,5")).fun.equals(AggregateFunction("")))

    assert(graphCube.getNearestDescendant(AggregateFunction("0,1,2,3"),AggregateFunction("1,2,3,4")).fun.equals(
      AggregateFunction("1,2,3")))
  }

  test("Get a cuboid in the graph cube"){
    assert(graphCube.get(AggregateFunction("1,2,3")).getOrElse(fail("None returned wrongly")).size === 25)
    assert(graphCube.get(AggregateFunction("3,4")).getOrElse(fail("None returned wrongly")).size === 20)
    assert(graphCube.get(AggregateFunction("")).getOrElse(fail("None returned wrongly")).size === 1000)

    graphCube.get(AggregateFunction("0,2")) match {
      case Some(cuboid) => fail("Wrongly returned " + cuboid)
      case None =>
    }

    graphCube.get(AggregateFunction("0")) match {
      case Some(cuboid) => fail("Wrongly returned " + cuboid)
      case None =>
    }

    graphCube.get(AggregateFunction("0,1,2")) match {
      case Some(cuboid) => fail("Wrongly returned " + cuboid)
      case None =>
    }
  }
}
