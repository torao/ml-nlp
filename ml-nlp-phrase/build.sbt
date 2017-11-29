organization := "at.hazm"

name := "ml-nlp-phrase"

version := "1.0.0." + new java.text.SimpleDateFormat("yyyyMMddHHmm").format(new java.util.Date())

scalaVersion := "2.12.4"

libraryDependencies ++= Seq(
  "at.hazm" %% "ml-nlp-corpus" % "1.0.0.+"
)
