package sgc.cuboid

import sgc.graph.ArrayVertexID

class AggregateFunction (val dimToAggregate : Seq[Int]) extends Serializable {

  /**
   *
   * @param dim  index of a dimension
   * @return     true if the input dim is aggregated with this function
   */
  def isAggregated(dim : Int) : Boolean = {
    dimToAggregate.contains(dim)
  }

  def aggregate(vertexOrEdge : Array[ArrayVertexID])    = {
    def toAggregateList(index : Int) : List[String] = {
      if (index == vertexOrEdge.length) return Nil
      aggregateVertex(vertexOrEdge(index)).toString("₠") :: toAggregateList(index + 1)
    }
    toAggregateList(0).mkString("ϱ")
  }

  def aggregateVertex(vertex : ArrayVertexID) = {
    for(dim <- dimToAggregate){
      vertex.setDimension(dim, vertex.getDimension(dim).getAggregate )
    }
    vertex
  }

  /**
   ** Return true if this function is a descendant of parameter function
   ** Descendant means that for every aggregated dimension of this, the dimension
   ** is aggregated in the input function
   ** 
   ** @param inputFunction Function to be compared
   **/
  def isDescendant(fun : AggregateFunction) : Boolean = {
    for(dim <- dimToAggregate){
      if(!fun.isAggregated(dim)) return false
    }
    true
  }

  override def toString = {
    dimToAggregate.mkString(",")
  }

  override def equals(that : Any) : Boolean = {
    that match {
      case fun : AggregateFunction => fun.dimToAggregate.equals(this.dimToAggregate)
      case _ => false
    }
  }
  
}

object AggregateFunction {
    def apply(stringRep : String) : AggregateFunction= {
      if(stringRep.length == 0) return new AggregateFunction(List())
      val explode = stringRep.split(",")
      new AggregateFunction(explode.iterator.toList.map(_.toInt))
    }
}



