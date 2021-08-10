import sbt.Keys.javaOptions

name := "metrics-exporter"
version := "3.0"
scalaVersion := "2.11.8"

val extJvmOpts = Seq(
  "-Xmx6g",
  "-Xms6g"
)

libraryDependencies ++= Dependencies.rootDependencies

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case _ => MergeStrategy.first
}
mainClass in assembly := Some("benchmark.metrics.exporter.ExporterMain")
mainClass in(Compile, run) := Some("benchmark.metrics.exporter.ExporterMain")

// JVM options
javaOptions in Universal ++= extJvmOpts.map(opt => s"-J$opt")
javaOptions in Test ++= extJvmOpts
// Docker configs
javaOptions in Docker ++= extJvmOpts.map(opt => s"-J$opt")
packageName in Docker := "metrics-exporter"

enablePlugins(JavaAppPackaging)
