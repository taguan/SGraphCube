package sgc.materialization.test

import org.scalatest.FunSuite

import sgc.materialization.CombinationsGenerator

class CombinationsGeneratorSuite extends FunSuite {

  test("combinations"){
    val combinations = CombinationsGenerator.comb(2,3)
    val expected = List(List(0,1),List(0,2),List(1,2))
    assert(combinations === expected)
  }
}
