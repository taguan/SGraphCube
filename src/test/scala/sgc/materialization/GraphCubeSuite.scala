package sgc.materialization

import org.scalatest.{BeforeAndAfter, FunSuite}
import sgc.cuboid.{Cuboid, CuboidEntry}


class GraphCubeSuite extends FunSuite with BeforeAndAfter {

  var graphCube : GraphCube = _

  before {
    graphCube = new GraphCube(6,3,CuboidEntry(Cuboid(""),1000,null))
    graphCube.addCuboid(CuboidEntry(Cuboid("0,2,3"),20,null))
    graphCube.addCuboid(CuboidEntry(Cuboid("1,2,3"),25,null))
    graphCube.addCuboid(CuboidEntry(Cuboid("3,4"),20,null))
    graphCube.addCuboid(CuboidEntry(Cuboid("1,2"),10,null))
    graphCube.addCuboid(CuboidEntry(Cuboid("0,1"),15,null))
  }

  /*
   * Method generateOrGetCuboid should be tested but needs to actually run Spark because
    * it uses StorageLevel
    * TODO
   */

  test("Get base cuboid"){
    assert(graphCube.getBaseCuboid.fun.equals(Cuboid("")))
  }

  test("Get Nearest Descendant"){
    assert(graphCube.getNearestDescendant(Cuboid("0,1,2,3")).fun.equals(Cuboid("0,2,3")))
    assert(graphCube.getNearestDescendant(Cuboid("1,2,3,4")).fun.equals(Cuboid("1,2,3")))
    assert(graphCube.getNearestDescendant(Cuboid("2,3,4,5")).fun.equals(Cuboid("3,4")))
    assert(graphCube.getNearestDescendant(Cuboid("1,3,5")).fun.equals(Cuboid("")))

    assert(graphCube.getNearestDescendant(Cuboid("0,1,2,3"),Cuboid("1,2,3,4")).fun.equals(
      Cuboid("1,2,3")))
  }

  test("Get a cuboid in the graph cube"){
    assert(graphCube.get(Cuboid("1,2,3")).getOrElse(fail("None returned wrongly")).size === 25)
    assert(graphCube.get(Cuboid("3,4")).getOrElse(fail("None returned wrongly")).size === 20)
    assert(graphCube.get(Cuboid("")).getOrElse(fail("None returned wrongly")).size === 1000)

    graphCube.get(Cuboid("0,2")) match {
      case Some(cuboid) => fail("Wrongly returned " + cuboid)
      case None =>
    }

    graphCube.get(Cuboid("0")) match {
      case Some(cuboid) => fail("Wrongly returned " + cuboid)
      case None =>
    }

    graphCube.get(Cuboid("0,1,2")) match {
      case Some(cuboid) => fail("Wrongly returned " + cuboid)
      case None =>
    }
  }
}
