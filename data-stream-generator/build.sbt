import sbt.Keys.javaOptions

name := "ndw-publisher"

version := "3.0"

scalaVersion := "2.11.8"
dockerBaseImage := "openjdk:8-jdk"
val extJvmOpts = Seq(
	"-J-Xmx5g",
	"-J-Xms5g"
)

libraryDependencies ++= Dependencies.rootDependencies

assemblyMergeStrategy in assembly := {
	case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
	case _ => MergeStrategy.first
}
mainClass in assembly := Some("ingest.StreamProducer")
mainClass in(Compile, run) := Some("ingest.StreamProducer")

// JVM options
javaOptions in Universal ++= extJvmOpts
javaOptions in Test ++= extJvmOpts
// Docker configs
javaOptions in Docker ++= extJvmOpts
packageName in Docker := "344178916407.dkr.ecr.eu-central-1.amazonaws.com/ndw-publisher"
maintainer in Docker := "Giselle van Dongen <giselle.vandongen@klarrio.com>"
packageSummary in Docker := "Stream producer for NDW traffic information stream"
packageDescription := "ndw-publisher"

enablePlugins(JavaAppPackaging)


