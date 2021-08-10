enablePlugins(JavaAppPackaging)
name := "stream-processing-evaluator"

scalaVersion := "2.12.8"

libraryDependencies ++= Dependencies.evaluation ++ Dependencies.typeSafe
//dependencyOverrides ++= Dependencies.jacksonDependencyOverrides

mainClass in(Compile, run) := Some("evaluation.EvaluationMain")
mainClass in assembly := Some("evaluation.EvaluationMain")
assemblyMergeStrategy in assembly := {
	case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
	case _ => MergeStrategy.first
}
