package sgc.cuboid

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter
import sgc.graph._

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

  test("Aggregate"){
    val dimensionsVal = new Array[Dimension](4)
    dimensionsVal(0) = new StringDimension("15")
    dimensionsVal(1) = new StringDimension("male")
    dimensionsVal(2) = new StringDimension("Belgium")
    dimensionsVal(3) = new StringDimension("<21")
    val vertex = new ArrayVertexID(dimensionsVal,4)
    val unaryArray = new Array[ArrayVertexID](1)
    unaryArray(0) = vertex
    assert(fun.aggregate(unaryArray) === "*₠male₠*₠*")

    val dimensionsVal2 = new Array[Dimension](4)
    dimensionsVal2(0) = new StringDimension("18")
    dimensionsVal2(1) = new StringDimension("female")
    dimensionsVal2(2) = new StringDimension("France")
    dimensionsVal2(3) = new StringDimension("<21")
    val vertex2 = new ArrayVertexID(dimensionsVal2,4)
    val pair = new Array[ArrayVertexID](2)
    pair(0) = vertex
    pair(1) = vertex2
    assert(fun.aggregate(pair) === "*₠male₠*₠*ϱ*₠female₠*₠*")
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

