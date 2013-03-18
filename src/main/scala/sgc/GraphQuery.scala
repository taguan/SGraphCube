package sgc

import spark.RDD
import java.util.Scanner

class GraphQuery(graph : RDD[Pair[String,Long]], reader : Scanner) {

  def interact(){
    var stop = false
    while(!stop){
      println("Waiting for a query on the selected graph.")
      println("first")
      println("count")
      println("save")
      println("quit")

      val startCommand = System.currentTimeMillis()

      reader.nextLine() match {
        case "first" => first()
        case "count" => count()
        case "save" => save()
        case "quit" => stop = true
        case _ => println("Unrecognized command")
      }

      println("Time elapsed : " + (System.currentTimeMillis() - startCommand))
    }
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
