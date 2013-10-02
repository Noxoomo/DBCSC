name := "DBCSC"

version := "0.01"

scalaVersion := "2.10.2"

seq(com.github.retronym.SbtOneJar.oneJarSettings: _*)

libraryDependencies += "commons-lang" % "commons-lang" % "2.6"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0.M8" % "test"

libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.2.1"

libraryDependencies += "com.typesafe.akka" %% "akka-camel" % "2.2.1" 
 
libraryDependencies += "com.typesafe.akka" %% "akka-remote" % "2.2.1"


