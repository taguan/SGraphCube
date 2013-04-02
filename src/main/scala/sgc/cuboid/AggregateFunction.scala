package sgc.cuboid

import sgc.graph.ArrayVertexID

class AggregateFunction (val dimToAggregate : Seq[Int]) extends Serializable {

  /**
   * Tells if a dimension is aggregated within this function
   * @param dim  index of a dimension
   * @return     true if the input dim is aggregated with this function
   */
  def isAggregated(dim : Int) : Boolean = {
    dimToAggregate.contains(dim)
  }

  /**
   * Uses AggregateVertex once or twice in order to aggregate the vertex
   * or the edge passed as parameter
   * @param vertexOrEdge  Vertex or edge to be aggregated
   * @return String representation of the aggregated vertex or edge, with  ₠ as dimension
   *         delimiter and ϱ as vertex delimiter
   */
  def aggregate(vertexOrEdge : Array[ArrayVertexID]) : String   = {
    def toAggregateList(index : Int) : List[String] = {
      if (index == vertexOrEdge.length) return Nil
      aggregateVertex(vertexOrEdge(index)).toString("₠") :: toAggregateList(index + 1)
    }
    toAggregateList(0).mkString("ϱ")
  }

  /**
   * Aggregates a vertex following this aggregate function
   * @param vertex vertex to be aggregated
   * @return the aggregated vertex (same object as input)
   */
  def aggregateVertex(vertex : ArrayVertexID) = {
    val newVertex = vertex.clone()
    for(dim <- dimToAggregate){
      newVertex.setDimension(dim, vertex.getDimension(dim).getAggregate )
    }
    newVertex
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

  /**
   * Aggregate an entry (a vertex or an edge) following the crossboid algorithm.
   * For a vertex, aggregates it with fun1 and fun2
   * For a edge, aggregates the first vertex with fun1, the second with fun2 and then
   * inversely
   * Vertex aggregated with fun1 appears before vertex aggregated with fun2
   * @param fun1  An aggregate function (cuboid)
   * @param fun2  Another aggregate function
   * @param entry A vertex or Edge
   * @return  The two aggregated vertices / edges
   */
  def crossAggregate(fun1: AggregateFunction, fun2: AggregateFunction, entry: Array[ArrayVertexID]): Pair[String,String] = {
    if (entry.length == 1){ // this is a vertex
      return Pair(fun1.aggregateVertex(entry(0)).toString("₠"),fun2.aggregateVertex(entry(0)).toString("₠"))
    }
    else {   //this is an edge
      return Pair(
        Seq(fun1.aggregateVertex(entry(0)).toString("₠"),fun2.aggregateVertex(entry(1)).toString("₠")).mkString("ϱ"),
        Seq(fun1.aggregateVertex(entry(1)).toString("₠"),fun2.aggregateVertex(entry(0)).toString("₠")).mkString("ϱ")
      )
    }
  }

  def apply(stringRep : String) : AggregateFunction= {
      if(stringRep.length == 0) return new AggregateFunction(List())
      val explode = stringRep.split(",")
      new AggregateFunction(explode.iterator.toList.map(_.toInt))
    }
}



