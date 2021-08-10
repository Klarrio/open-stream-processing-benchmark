import sbt.Keys.javaOptions

name := "benchmark-output-consumer"

scalaVersion := "2.12.8"

val extJvmOpts = Seq(
	"-Xmx8g",
	"-Xms8g"
)

libraryDependencies ++= Dependencies.rootDependencies

assemblyMergeStrategy in assembly := {
	case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
	case _ => MergeStrategy.first
}
mainClass in assembly := Some("output.consumer.OutputConsumer")
mainClass in(Compile, run) := Some("output.consumer.OutputConsumer")

// JVM options
javaOptions in Universal ++= extJvmOpts.map(opt => s"-J$opt")
javaOptions in Test ++= extJvmOpts
// Docker configs
javaOptions in Docker ++= extJvmOpts.map(opt => s"-J$opt")
maintainer in Docker := "Giselle van Dongen <giselle.vandongen@klarrio.com>"
packageSummary in Docker := "Metrics consumer for stream processing benchmark"
packageDescription := "output-consumer"


enablePlugins(JavaAppPackaging)


