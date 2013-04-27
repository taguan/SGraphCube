package sgc

import spark.{SparkContext, RDD}
import java.util.Scanner
import spark.bagel._

class GraphQuery(graph : RDD[Pair[String,Long]], reader : Scanner, sc : SparkContext) {

  def interact(){
    var stop = false
    while(!stop){
      println("Waiting for a query on the selected graph.")
      println("slice")
      println("first")
      println("count")
      println("save")
      println("quit")

      val startCommand = System.currentTimeMillis()

      reader.nextLine() match {
        case "slice" => sliceDice()
        case "first" => first()
        case "count" => count()
        case "save" => save()
        case "quit" => stop = true
        case _ => println("Unrecognized command")
      }

      println("Time elapsed : " + (System.currentTimeMillis() - startCommand))
    }
  }

  def sliceDice(){
    val vertices = BagelProcessing.generateVertices(graph).cache()
    val firstVertex = vertices.first()._1
    println("First vertex : " + firstVertex)
    val initialMsg = sc.parallelize(Array(
      Pair(firstVertex, new SliceDiceMessage(firstVertex))))

    println("How many supersteps ?")
    val result = Bagel.run(sc, vertices, initialMsg, combiner = new SliceDiceCombiner(),
      numPartitions = sc.defaultParallelism) (BagelProcessing.compute(reader.nextLine().toInt))
    .filter(entry => entry._2.toReturn)

    println("Output path to save ?")
    result.map(entry => entry._2).saveAsTextFile(reader.nextLine())

  }

  def first(){
    println(graph.first())
  }

  def count(){
    println(graph.count())
  }

  def save(){
    println("Output path to save ?")
    graph.map(entry => (entry._1 + "\t" + entry._2)).saveAsTextFile(reader.nextLine())
  }

}
