import sbt.Keys.javaOptions

name := "data-stream-generator"

version := "1.0"

scalaVersion := "2.11.8"

val extJvmOpts = Seq(
  "-Xmx6g",
  "-Xms6g",
  "-XX:ParallelGCThreads=8 ",
  "-XX:+UseConcMarkSweepGC"
)

libraryDependencies ++= Dependencies.rootDependencies

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case _ => MergeStrategy.first
}
mainClass in assembly := Some("ingest.LocalStreamProducer")
mainClass in(Compile, run) := Some("ingest.LocalStreamProducer")

// JVM options
javaOptions in Universal ++= extJvmOpts.map(opt => s"-J$opt")
javaOptions in Test ++= extJvmOpts
// Docker configs
javaOptions in Docker ++= extJvmOpts.map(opt => s"-J$opt")
packageName in Docker := "data-stream-generator"
maintainer in Docker := "Giselle van Dongen <giselle.vandongen@klarrio.com>"
packageSummary in Docker := "Stream producer for Open Stream Processing Benchmark"
packageDescription := "data-stream-generator"


enablePlugins(JavaAppPackaging)



