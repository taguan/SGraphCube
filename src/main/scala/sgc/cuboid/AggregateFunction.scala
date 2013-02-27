package sgc.cuboid

import sgc.graph.ArrayVertexID

class AggregateFunction (dimToAggregate : Seq[Int]) {
 
  def isAggregated(dim : Int) : Boolean = {
    dimToAggregate.contains(dim)
  }

  def aggregateVertex(vertex : ArrayVertexID) = {
    for(dim <- dimToAggregate){
      vertex.setDimension(dim, vertex.getDimension(dim).getAggregate() )
    }
  }

  /**
   ** Return true if this function is a descendant of parameter function
   ** Descendant means that for every aggregated dimension of this, the dimension
   ** is aggregated in the input function
   ** 
   ** @param inputFunction Function to be compared
   **/
  def isDescendant(fun : AggregateFunction) = {
    for(dim <- dimToAggregate){
      if(!fun.isAggregated(dim)) false
    }
    true
  }

  override def toString() = {
    dimToAggregate.mkString(",")
  }
  
}

object AggregateFunction {
    def apply(stringRep : String) : AggregateFunction= {
      if(stringRep.length == 0) return new AggregateFunction(List())
      val explode = stringRep.split(",")
      new AggregateFunction(explode.iterator.toList.map(_.toInt))
    }
}



