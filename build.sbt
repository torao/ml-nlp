organization := "at.hazm"

name := "nlp-ml"

version := "1.0"

scalaVersion := "2.12.2"

resolvers ++= Seq(
  "CodeLibs Repository" at "http://maven.codelibs.org/"
)

libraryDependencies ++= Seq(
  "org.codelibs" % "lucene-analyzers-kuromoji-ipadic-neologd" % "5.3.1-20151231",
  "org.deeplearning4j" % "deeplearning4j-nlp" % "0.8.0",
  "org.deeplearning4j" % "deeplearning4j-nlp-japanese" % "0.8.0",
  "org.deeplearning4j" % "deeplearning4j-nlp-uima" % "0.8.0",
  "org.nd4j" % "nd4j-native-platform" % "0.8.0",
  "org.xerial" % "sqlite-jdbc" % "3.16.1"
)
