package sgc

import graph.VertexIDParser
import spark.bagel._
import spark.{RDD, KryoRegistrator}
import com.esotericsoftware.kryo.Kryo
import spark.SparkContext._

object BagelProcessing {

  def compute(maxDiameter : Int)
             (self : SliceDiceVertex, originators : Option[Set[String]], superstep : Int
               ): (SliceDiceVertex, Array[SliceDiceMessage]) = {
    if (originators == None){ //desactivate it
      return(new SliceDiceVertex(self.dimensionValues, self.weight, self.outEdges,false, self.toReturn),
        Array[SliceDiceMessage]())
    }

    if (superstep >= maxDiameter){   //time to finish
      return(new SliceDiceVertex(self.dimensionValues, self.weight, self.outEdges,false, true),
        Array[SliceDiceMessage]())
    }
    val alreadyVisitedVertices = originators.getOrElse(Set[String]()) + self.dimensionValues

    val destinationVertices = self.outEdges.map(entry => entry._1) -- alreadyVisitedVertices

    val outbox : Array[SliceDiceMessage] =
      destinationVertices.map(vertexId => new SliceDiceMessage(vertexId, alreadyVisitedVertices)).toArray

    (new SliceDiceVertex(self.dimensionValues, self.weight, self.outEdges, false, true), outbox)
  }

  def generateVertices(aggregateNetwork : RDD[Pair[String,Long]]) : RDD[(String,SliceDiceVertex)] = {
    val inter : RDD[(String, SliceDiceVertex)] = aggregateNetwork.flatMap(entry => {
      val vertexOrEdge = VertexIDParser.parseID(entry._1)
      if (vertexOrEdge.length == 1){ // it is a vertex
        Seq(Pair(entry._1, new SliceDiceVertex(entry._1,entry._2)))
      }
      else{ //it is an edge, we duplicate it
        val vertex1 = vertexOrEdge(0).toString("₠")
        val vertex2 = vertexOrEdge(1).toString("₠")
        Seq(Pair(vertex1, new SliceDiceVertex(vertex1,0,Set(Pair(vertex2,entry._2)))),
          Pair(vertex2, new SliceDiceVertex(vertex2,0,Set(Pair(vertex1,entry._2)))))
      }
    })
    inter.reduceByKey((vertex1,vertex2) => vertex1.merge(vertex2))
  }


}

class SliceDiceVertex() extends Vertex  with Serializable {
  var dimensionValues : String = _
  var weight : Long = _
  var outEdges : Set[(String,Long)] = _
  var active : Boolean = _
  var toReturn : Boolean = _

  def this(dimensionValues : String, weight : Long, outEdges : Set[(String,Long)] = Set(),
           active : Boolean = false, toReturn : Boolean = false)  {
    this()
    this.dimensionValues = dimensionValues
    this.weight = weight
    this.outEdges = outEdges
    this.active = active
    this.toReturn = toReturn
  }

  def addEdge(edges : Set[(String,Long)]){
    this.outEdges = this.outEdges ++ edges
  }

  def merge(other : SliceDiceVertex) : SliceDiceVertex = {
    this.weight = this.weight + other.weight
    this .addEdge(other.outEdges)
    this
  }

  override def toString = {
    val strb = new StringBuilder()
    strb.append(dimensionValues + "  " + weight)
    strb.append("\n")
    for (edge <- outEdges){
      strb.append("\t")
      strb.append(edge)
    }
    strb.toString()
  }
}

class SliceDiceMessage() extends Message[String] with Serializable {
  var targetId : String = _
  var originators : Set[String] = _

  def this(targetId : String, originators : Set[String] = Set()){
    this()
    this.targetId = targetId
    this.originators = originators
  }
}

class SliceDiceCombiner extends Combiner[SliceDiceMessage,Set[String]] with Serializable {
  def createCombiner(msg : SliceDiceMessage) : Set[String] = {
    msg.originators
  }
  //Set definition ensures that there will be no duplicated
  def mergeMsg(combiner : Set[String], msg : SliceDiceMessage) : Set[String] = {
    combiner ++ msg.originators
  }
  def mergeCombiners(c1 : Set[String], c2 : Set[String]) : Set[String] = {
    c1 ++ c2
  }
}

class SliceDiceKryoRegistrator extends KryoRegistrator {
  def registerClasses(kryo: Kryo) {
    kryo.register(classOf[SliceDiceVertex])
    kryo.register(classOf[SliceDiceMessage])
  }
}
