name := "DBCSC"

version := "0.01"

scalaVersion := "2.10.2"

seq(com.github.retronym.SbtOneJar.oneJarSettings: _*)

libraryDependencies += "commons-lang" % "commons-lang" % "2.6"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0.M8" % "test"
