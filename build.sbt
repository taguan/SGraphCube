name := "SGraph Cube"

version := "1.0"

scalaVersion := "2.9.2"

libraryDependencies += "org.spark-project" %% "spark-core" % "0.7.0"

libraryDependencies += "org.spark-project" % "spark-bagel_2.9.2" % "0.7.0"

libraryDependencies += "commons-cli" % "commons-cli" % "1.2"

libraryDependencies += "org.scalatest" %% "scalatest" % "1.6.1" % "test"

resolvers ++= Seq(
    "Akka Repository" at "http://repo.akka.io/releases/",
    "Spray Repository" at "http://repo.spray.cc/")
