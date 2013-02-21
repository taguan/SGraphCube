package main.scala.sgc

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

  }

}
