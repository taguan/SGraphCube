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
      println("quit")

      reader.nextLine() match {
        case "first" => first()
        case "count" => count()
        case "quit" => stop = true
        case _ => println("Unrecognized command")
      }
    }
  }

  def first(){
    println(graph.first())
  }

  def count(){
    println(graph.count())
  }

}
