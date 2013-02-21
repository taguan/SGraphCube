package main.scala.sgc

import java.io.IOException

import spark.KryoRegistrator
import spark.SparkContext
import SparkContext._

import org.apache.commons.cli._

object SGraphCube {

  def main(args : Array[String]) = {

    /**
     * Defines options for the command line
     * parser
     */
    def getOptions() : Options = {
      val options = new Options()
      options.addOption("h","help",false,"Display help")
      options.addOption("inp","inputPath",true,"Base graph input path, for " +
        "example : hdfs://namenode:54310/yourhdfspath")
        options.addOption("k","maxCuboids",true,"Maximum number of cuboids to materialize")
        options.addOption("ml","minLevel",true,"Starting dimension for the MinLevel algorithm")
        options.addOption("sc","sparkContext",true,"Spark Context argument, for "
          + "default : local[2]")
        options.addOption("sh","sparkHome",true,"Path to spark home installation, default : .")
        options.addOption("jar","jar",true,"Project jar, default : " +
          "target/scala-2.9.2/sgraph-cube_2.9.2-1.0.jar")
        options
    }

    /**
     * Prints a well formatted help
     */
    def printHelp() = {
      val formatter = new HelpFormatter()
      formatter.printHelp("SGraphCube",getOptions(),true)
    }

    val options = getOptions()
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
    }

    /**
     * Initialization of the spark environment
     */ 
    System.setProperty("spark.serializer", "spark.KryoSerializer")
    val sc = new SparkContext(cmd.getOptionValue("sc","local[2]"),"SGraphCube",
      cmd.getOptionValue("sh","."), List(cmd.getOptionValue("jar",
        "target/scala-2.9.2/sgraph-cube_2.9.2-1.0.jar")))

    val inputGraph = sc.textFile(cmd.getOptionValue("inp")).map(parseLine(_))

    println(inputGraph.count())
  }


  /**
   * Parses a key value Pair from a line around a tab character
   * @throw IOException
   */
  def parseLine(line : String) = {
    val regex = """([^\t]+)\t([^\t]+)""".r

    line match {
      case regex(key,value) => Pair(key,value.toLong)
      case _ => throw new IOException("Wrongly formatted line, line : " + line)
    }
  }

}
