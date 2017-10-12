organization := "at.hazm"

name := "ml-nlp-extract"

version := "1.0.0"

scalaVersion := "2.12.3"

libraryDependencies ++= Seq(
  "at.hazm" %% "ml-nlp-corpus" % "1.0.+",
  "org.apache.commons" % "commons-compress" % "1.+",  // bzip2
  "com.h2database" % "h2" % "1.4.+",
  "org.slf4j" % "slf4j-log4j12" % "1.7.+",
  "org.specs2" %% "specs2-core" % "3.8.+" % "test"
)
