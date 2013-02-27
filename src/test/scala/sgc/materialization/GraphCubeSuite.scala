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

  test("Get base cuboid"){
    assert(graphCube.getBaseCuboid.fun.equals(AggregateFunction("")))
  }

  test("Get Nearest Descendant"){
    assert(graphCube.getNearestDescendant(AggregateFunction("0,1,2,3")).fun.equals(AggregateFunction("0,2,3")))
    assert(graphCube.getNearestDescendant(AggregateFunction("1,2,3,4")).fun.equals(AggregateFunction("1,2,3")))
    assert(graphCube.getNearestDescendant(AggregateFunction("2,3,4,5")).fun.equals(AggregateFunction("")))  //minLevel property
  }
}
