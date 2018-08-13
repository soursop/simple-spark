name := "simple_spark"

version := "0.1"

scalaVersion := "2.11.0"

resolvers ++= Seq(
  "CDH4" at "https://repository.cloudera.com/artifactory/cloudera-repos/"
)

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.1"
libraryDependencies += "org.apache.hadoop" % "hadoop-core" % "2.6.0-mr1-cdh5.15.0"
//libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "2.6.0-mr1-cdh5.15.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.0-SNAP10" % Test