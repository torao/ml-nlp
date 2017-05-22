organization := "at.hazm"

name := "nlp-ml-corpus"

version := "1.0.0"

scalaVersion := "2.12.2"

libraryDependencies ++= Seq(
  "org.scala-lang.modules" %% "scala-xml" % "1.0.6",
  "org.xerial" % "sqlite-jdbc" % "3.16.1",
  "org.xerial.snappy" % "snappy-java" % "1.1.2.6",
  "org.apache.commons" % "commons-compress" % "1.8",
  "com.typesafe.play" % "play-json_2.12" % "2.6.0-M7",
  "jline" % "jline" % "2.14.3",
  "org.slf4j" % "slf4j-log4j12" % "1.7.25",
  "org.specs2" %% "specs2-core" % "3.8.9"
)
