package sgc

import java.io.IOException

import spark.{Logging, SparkContext}
import materialization._
import cuboid._

import org.apache.commons.cli._
import spark.storage.StorageLevel
import java.util.Scanner

object SGraphCube extends Logging{

  def main(args : Array[String]) {

    /**
     * Defines options for the command line
     * parser
     */
    def getOptions : Options = {
      val options = new Options()
      options.addOption("h","help",false,"Display help")
      options.addOption("inp","inputPath",true,"Base graph input path, for " +
        "example : hdfs://namenode:54310/yourhdfspath")
      options.addOption("k","maxCuboids",true,"Maximum number of cuboids to materialize")
      options.addOption("ml","minLevel",true,"Starting dimension for the MinLevel algorithm")
      options.addOption("n", "dimensions", true, "Number of dimensions")
      options.addOption("sc","sparkContext",true,"Spark Context argument, for "
        + "default : local[2]")
      options.addOption("sh","sparkHome",true,"Path to spark home installation, default : .")
      options.addOption("jar","jar",true,"Project jar, default : " +
        "target/scala-2.9.2/sgraph-cube_2.9.2-1.0.jar")
      options.addOption("p","persist",false,"Cache the input graph while materializing")
      options
    }

    /**
     * Prints a well formatted help
     */
    def printHelp() {
      val formatter = new HelpFormatter()
      formatter.printHelp("SGraphCube",getOptions,true)
    }

    val options = getOptions
    val parser = new BasicParser()
    val cmd = parser.parse(options, args)

    if(args.length == 0 || cmd.hasOption("h")){
      printHelp()
      sys.exit(0)
    }

    if(!cmd.hasOption("inp")){
      println("Input path required")
      printHelp()
      sys.exit(0)
    }

    if(!cmd.hasOption("k")){
      println("Maximum number of cuboids required")
      printHelp()
      sys.exit(0)
    }

    if(!cmd.hasOption("ml")){
      println("Min Level starting point required")
      printHelp()
      sys.exit(0)
    }

    if (!cmd.hasOption("n")){
      println("Number of dimensions required")
      printHelp()
      sys.exit(0)
    }
    val numberOfDimensions =  cmd.getOptionValue("n").toInt

    /**
     * Initialization of the spark environment
     */ 
    System.setProperty("spark.serializer", "spark.KryoSerializer")

    val sc = new SparkContext(cmd.getOptionValue("sc","local[2]"),"SGraphCube",
      cmd.getOptionValue("sh","."), List(cmd.getOptionValue("jar",
        "target/scala-2.9.2/sgraph-cube_2.9.2-1.0.jar")))

    val inputGraph = sc.textFile(cmd.getOptionValue("inp")).map(parseLine(_))

    if (cmd.hasOption("p")) {
      logInfo("Persisting input graph")
      inputGraph.persist((StorageLevel.MEMORY_ONLY_SER))
    }

    /**
     * Materialization step
     */
    val startMaterialization = System.currentTimeMillis()
    val cube = MinLevelStrategy.materialize(cmd.getOptionValue("k").toInt,cmd.getOptionValue("ml").toInt,
                numberOfDimensions,CuboidEntry(AggregateFunction(""),Long.MaxValue,inputGraph))
    println("Materialization time : " + (System.currentTimeMillis() - startMaterialization))

    val reader = new Scanner(System.in)
    var stop = false

    /**
     * Interactive querying
     */
    while(!stop) {
      println("Waiting for a query.")
      println("cuboid")
      println("crossboid")
      println("quit")

      reader.nextLine() match {
        case "cuboid" => interactiveCuboid()
        case "crossboid" => interactiveCrossboid()
        case "quit" => stop = true
        case _ => println("Unrecognized command")
      }
    }

    def interactiveCuboid() {
      println("Aggregate function ? Ex : 0,2 (type \"base\" for the base cuboid)")
      val regex = """\d+(,\d+)*""".r
      val userEntry = reader.nextLine()
      userEntry match{
        case regex(_)  => {
          val fun = AggregateFunction(userEntry)
          val descendant = cube.getNearestDescendant(fun).cuboid
          val requestedGraph = CuboidQuery.generateCuboid(descendant, fun, numberOfDimensions)
          requestedGraph.persist(StorageLevel.MEMORY_ONLY)

          val graphAnalyser = new  GraphQuery(requestedGraph,reader)
          graphAnalyser.interact()
        }
        //if you want to interact directly with the input graph
        case "base" => {
          val fun = AggregateFunction("")
          val descendant = cube.getBaseCuboid.cuboid
          val requestedGraph = CuboidQuery.generateCuboid(descendant, fun, numberOfDimensions)

          val graphAnalyser = new  GraphQuery(requestedGraph,reader)
          graphAnalyser.interact()
        }
        case _ => println("wrongly formatted aggregate function")
      }
    }

    def interactiveCrossboid() {
      println("First aggregate function ? Ex : 0,2" +
        "base for the base cuboid")
      val regex = """\d+(,\d+)*""".r
      val userEntry = reader.nextLine()
      userEntry match{
        case regex(_)  => {
          val fun = AggregateFunction(userEntry)
          val descendant = cube.getNearestDescendant(fun).cuboid
          val requestedGraph = CuboidQuery.generateCuboid(descendant, fun, numberOfDimensions)
          requestedGraph.persist(StorageLevel.MEMORY_ONLY)

          val graphAnalyser = new  GraphQuery(requestedGraph,reader)
          graphAnalyser.interact()
        }
        //if you want to interact directly with the input graph
        case "base" => {
          val fun = AggregateFunction("")
          val descendant = cube.getBaseCuboid.cuboid
          val requestedGraph = CuboidQuery.generateCuboid(descendant, fun, numberOfDimensions)

          val graphAnalyser = new  GraphQuery(requestedGraph,reader)
          graphAnalyser.interact()
        }
        case _ => println("wrongly formatted aggregate function")
      }
    }

    sc.stop()
  }


  /**
   * Parses a key value Pair from a line around a tab character
   */
  def parseLine(line : String) = {
    val regex = """([^\t]+)\t([^\t]+)""".r

    line match {
      case regex(key,value) => Pair(key,value.toLong)
      case _ => throw new IOException("Wrongly formatted line, line : " + line)
    }
  }


}

