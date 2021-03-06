version := "0.1"
scalaVersion := "2.11.12"
name := "hello-world"
organization := "ch.epfl.scala"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.3.0"
libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.25"