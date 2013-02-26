package sgc.cuboid.test

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter
import main.scala.sgc.cuboid.AggregateFunction
import main.java.sgc.graph._

class AggregateFunctionSuite extends FunSuite with BeforeAndAfter {

  var fun : AggregateFunction = _

  before {
    fun = AggregateFunction("0,2,3")
  }

  test("string apply"){
    assert(fun.toString() === "0,2,3")
    val emptyFun = AggregateFunction("")
    assert(emptyFun.toString() === "")
  }

  test("Is Aggregated"){
    assert(fun.isAggregated(0))
    assert(fun.isAggregated(2))
    assert(fun.isAggregated(3))
    assert(!fun.isAggregated(1))
  }

  test("Aggregate vertex"){
    val dimensionsVal = new Array[Dimension](4)
    dimensionsVal(0) = new StringDimension("15")
    dimensionsVal(1) = new StringDimension("male")
    dimensionsVal(2) = new StringDimension("Belgium")
    dimensionsVal(3) = new StringDimension("<21")
    val vertex = new ArrayVertexID(dimensionsVal,4)

    fun.aggregateVertex(vertex)

    assert(vertex.getDimension(0).toString === "*")
    assert(vertex.getDimension(1).toString === "male")
    assert(vertex.getDimension(2).toString === "*")
    assert(vertex.getDimension(3).toString === "*")
  }

  test("Is Descendant") {
    val fun1 = AggregateFunction("0,1,2,3")
    assert(fun.isDescendant(fun1))
    //assert(!fun1.isDescendant(fun))

    val fun2 = AggregateFunction("")
    assert(fun2.isDescendant(fun))

    val fun3 = AggregateFunction ("0,1")
    assert(!fun3.isDescendant(fun))

    val fun4 = AggregateFunction("0,2")
    assert(fun4.isDescendant(fun))
  }

}

