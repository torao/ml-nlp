organization := "at.hazm"

name := "ml-nlp-summarization"

version := "1.0.0." + new java.text.SimpleDateFormat("yyyyMMddHHmm").format(new java.util.Date())

scalaVersion := "2.12.4"

resolvers += "CodeLibs Repository" at "http://maven.codelibs.org/"

libraryDependencies ++= Seq(
  "net.sf.jung" % "jung-algorithms" % "2.1.1",
  "net.sf.jung" % "jung-graph-impl" % "2.1.1",
  "org.codelibs" % "elasticsearch-analysis-kuromoji-neologd" % "5.5.+",
  "org.slf4j" % "slf4j-log4j12" % "1.7.+",
  "org.specs2" %% "specs2-core" % "3.8.9" % "test"
)
