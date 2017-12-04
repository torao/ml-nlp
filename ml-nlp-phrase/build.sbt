organization := "at.hazm"

name := "ml-nlp-phrase"

version := "1.0.0." + new java.text.SimpleDateFormat("yyyyMMddHHmm").format(new java.util.Date())

scalaVersion := "2.12.4"

libraryDependencies ++= Seq(
  "at.hazm" %% "ml-nlp-corpus" % "1.0.0.+",
  "org.deeplearning4j" % "deeplearning4j-nlp" % "0.9.+",
  "org.nd4j" % "nd4j-native-platform" % "0.9.+",
  "org.slf4j" % "slf4j-log4j12" % "1.7.25",
  "org.specs2" %% "specs2-core" % "3.8.9"
)
