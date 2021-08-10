import sbt.Keys.javaOptions
import sbtassembly.AssemblyPlugin.autoImport.PathList

name := "stream-benchmark"
scalaVersion in ThisBuild := "2.12.8"
cancelable in Global := true


lazy val common = Project(id = "common-benchmark",
  base = file("common-benchmark"))
  .settings(libraryDependencies ++= Dependencies.commonDependencies,
    resolvers ++= Seq("otto-bintray" at "https://dl.bintray.com/ottogroup/maven"),
    test in assembly := {},
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
      case _ => MergeStrategy.first
    },
    fork in Test := true)

lazy val flink = Project(id = "flink-benchmark",
  base = file("flink-benchmark"))
  .dependsOn(common % "compile->compile;test->test")
  .settings(libraryDependencies ++= Dependencies.flinkDependencies,
    frameworkSettings("Flink", "3.0"))
  .enablePlugins(JavaAppPackaging)

lazy val kafka = Project(id = "kafka-benchmark",
  base = file("kafka-benchmark"))
  .dependsOn(common % "compile->compile;test->test") //fyi: http://stackoverflow.com/questions/8193904/sbt-test-dependencies-in-multiprojects-make-the-test-code-available-to-dependen
  .settings(libraryDependencies ++= Dependencies.kafkaDependencies,
    dockerBaseImage := "openjdk:8-jdk",
    javaOptions in Universal ++= Seq("-J-XX:+UseG1GC",
//      "-J-XX:InitiatingHeapOccupancyPercent=35",
//      "-J-XX:ParallelGCThreads=4",
//      "-J-XX:ConcGCThreads=2",
      "-J-XX:MaxGCPauseMillis=200",
//      "-J-Xmx15g",
//      "-J-Xms15g",
      "-J-XX:+PrintFlagsFinal",
      "-Dcom.sun.management.jmxremote=true",
      "-Dcom.sun.management.jmxremote.local.only=false",
      "-Dcom.sun.management.jmxremote.authenticate=false",
      "-Dcom.sun.management.jmxremote.ssl=false",
      "-Dcom.sun.management.jmxremote.rmi.port=8501",
      "-Dcom.sun.management.jmxremote.port=8500"),
    dockerExposedPorts := Seq(8500, 8501),
    frameworkSettings("Kafka", "3.0"))
  .enablePlugins(JavaAppPackaging)

lazy val spark = Project(id = "spark-benchmark",
  base = file("spark-benchmark"))
  .dependsOn(common % "compile->compile;test->test")
  .settings(
    libraryDependencies ++= Dependencies.sparkDependencies,
    frameworkSettings("Spark", "3.0"),
    parallelExecution in Test := false)
  .enablePlugins(JavaAppPackaging)

lazy val structuredStreaming = Project(id = "structured-streaming-benchmark",
  base = file("structured-streaming-benchmark"))
  .dependsOn(common % "compile->compile;test->test")
  .settings(
    libraryDependencies ++= Dependencies.structuredStreamingDependencies,
    frameworkSettings("StructuredStreaming", "3.0"))
  .enablePlugins(JavaAppPackaging)

def frameworkSettings(framework: String, versionDocker: String) = Seq(
  mainClass in assembly := Some(s"${framework.toLowerCase}.benchmark.${framework}TrafficAnalyzer"),
  mainClass in(Compile, run) := Some(s"${framework.toLowerCase}.benchmark.${framework}TrafficAnalyzer"),

  test in assembly := {},
  assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs@_*) => MergeStrategy.discard
    case x => MergeStrategy.first
  },
  version := versionDocker,
  fork in Test := true,
  envVars in Test := Map("DEPLOYMENT_TYPE" -> "local", "MODE" -> "constant-rate", "KAFKA_BOOTSTRAP_SERVERS" -> "localhost:9092")
)