organization := "at.hazm"

name := "ml-nlp-corpus"

version := "1.0.0." + new java.text.SimpleDateFormat("yyyyMMddHHmm").format(new java.util.Date())

scalaVersion := "2.12.4"

resolvers += "CodeLibs Repository" at "http://maven.codelibs.org/"

libraryDependencies ++= Seq(
  "org.scala-lang.modules" %% "scala-xml" % "1.0.6",
  "org.codelibs" % "elasticsearch-analysis-kuromoji-neologd" % "5.5.+",
  "org.apache.tomcat" % "tomcat-jdbc" % "8.5.+",
  "org.postgresql" % "postgresql" % "42.1.+",
  "com.h2database" % "h2" % "1.4.+",
  "org.xerial" % "sqlite-jdbc" % "3.16.+",
  "org.apache.commons" % "commons-compress" % "1.8",
  "com.typesafe.play" %% "play-json" % "2.6.0-M7",
  "org.deeplearning4j" % "deeplearning4j-nlp" % "0.9.+",
  "org.nd4j" % "nd4j-native-platform" % "0.9.+",
  "com.typesafe.akka" %% "akka-actor" % "2.+",
  "com.typesafe.akka" %% "akka-stream" % "2.+",
  "com.typesafe.akka" %% "akka-http" % "10.+",
  "com.typesafe.akka" %% "akka-http-spray-json" % "10.+",
  "org.slf4j" % "slf4j-log4j12" % "1.7.25",
  "org.specs2" %% "specs2-core" % "3.8.9" % "test"
)
