name := "Tennis"

version := "1.0"

scalaVersion := "2.10.5"

val sparkModules = List("spark-core",
                       "spark-streaming",
                       "spark-sql",
                       "spark-hive",
                       "spark-mllib",
                       "spark-repl",
                       "spark-graphx")
 
val sparkDeps = sparkModules.map( module => "org.apache.spark" % s"${module}_2.10" % "1.4.0" )
libraryDependencies ++= sparkDeps

mainClass in (Compile, packageBin) := Some("main.scala.com.ibm.scala.Alena")