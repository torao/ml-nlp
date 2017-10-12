organization := "at.hazm"

name := "ml-nlp-corpus"

version := "1.0.0"

scalaVersion := "2.12.2"

libraryDependencies ++= Seq(
  "org.xerial" % "sqlite-jdbc" % "3.16.1",
  "org.apache.commons" % "commons-compress" % "1.8",
  "com.typesafe.play" % "play-json_2.12" % "2.6.0-M7",
  "org.slf4j" % "slf4j-log4j12" % "1.7.25",
  "org.specs2" %% "specs2-core" % "3.8.9"
)
