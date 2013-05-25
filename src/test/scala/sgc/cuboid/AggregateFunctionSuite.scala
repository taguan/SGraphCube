package sgc.cuboid

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter
import sgc.graph._

class AggregateFunctionSuite extends FunSuite with BeforeAndAfter {

  var fun : Cuboid = _

  before {
    fun = Cuboid("0,2,3")
  }

  test("string apply"){
    assert(fun.toString() === "0,2,3")
    val emptyFun = Cuboid("")
    assert(emptyFun.toString() === "")
  }

  test("Crossboid aggregation"){
    val fun2 = Cuboid("1")

    val dimensionsVal = new Array[Dimension](4)
    dimensionsVal(0) = new StringDimension("15")
    dimensionsVal(1) = new StringDimension("male")
    dimensionsVal(2) = new StringDimension("Belgium")
    dimensionsVal(3) = new StringDimension("<21")
    val vertex = new ArrayVertexID(dimensionsVal,4)
    val unaryArray = new Array[ArrayVertexID](1)
    unaryArray(0) = vertex
    assert(Cuboid.crossAggregate(fun,fun2,unaryArray) === Pair("*₠male₠*₠*","15₠*₠Belgium₠<21") )

    val dimensionsVal2 = new Array[Dimension](4)
    dimensionsVal2(0) = new StringDimension("18")
    dimensionsVal2(1) = new StringDimension("female")
    dimensionsVal2(2) = new StringDimension("France")
    dimensionsVal2(3) = new StringDimension("<21")
    val vertex2 = new ArrayVertexID(dimensionsVal2,4)
    val pair = new Array[ArrayVertexID](2)
    pair(0) = vertex
    pair(1) = vertex2
    assert(Cuboid.crossAggregate(fun,fun2,pair) ===
      Pair("*₠male₠*₠*ϱ18₠*₠France₠<21","*₠female₠*₠*ϱ15₠*₠Belgium₠<21"))
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

    val newVertex = fun.aggregateVertex(vertex)

    assert(newVertex.getDimension(0).toString === "*")
    assert(newVertex.getDimension(1).toString === "male")
    assert(newVertex.getDimension(2).toString === "*")
    assert(newVertex.getDimension(3).toString === "*")
  }

  test("Is Descendant") {
    val fun1 = Cuboid("0,1,2,3")
    assert(fun.isDescendant(fun1))
    //assert(!fun1.isDescendant(cub))

    val fun2 = Cuboid("")
    assert(fun2.isDescendant(fun))

    val fun3 = Cuboid ("0,1")
    assert(!fun3.isDescendant(fun))

    val fun4 = Cuboid("0,2")
    assert(fun4.isDescendant(fun))
  }

}

