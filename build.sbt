organization := "at.hazm"

name := "nlp-ml"

version := "1.0.0"

scalaVersion := "2.12.3"

resolvers ++= Seq(
  "CodeLibs Repository" at "http://maven.codelibs.org/"
)

libraryDependencies ++= Seq(
  "at.hazm" %% "ml-nlp-corpus" % "1.0.+",
  "com.typesafe" % "config" % "1.3.1",
  "org.scala-lang.modules" %% "scala-xml" % "1.0.6",
  "org.codelibs" % "lucene-analyzers-kuromoji-ipadic-neologd" % "5.3.1-20151231",
  "org.deeplearning4j" % "deeplearning4j-nlp" % "0.8.0",
  "org.deeplearning4j" % "deeplearning4j-nlp-japanese" % "0.8.0",
  "org.deeplearning4j" % "deeplearning4j-nlp-uima" % "0.8.0",
  "org.nd4j" % "nd4j-native-platform" % "0.8.0",
  "org.xerial" % "sqlite-jdbc" % "3.16.1",
  "org.twitter4j" % "twitter4j-core" % "[4.0,)",
  "org.apache.commons" % "commons-compress" % "1.8",
  "com.typesafe.play" % "play-json_2.12" % "2.6.0-M7",
  "args4j" % "args4j" % "2.33",
  "jline" % "jline" % "2.14.3",
  "org.slf4j" % "slf4j-log4j12" % "1.7.25",
  "org.specs2" %% "specs2-core" % "3.8.9"
)
