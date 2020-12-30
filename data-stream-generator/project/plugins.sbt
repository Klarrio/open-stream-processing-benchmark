logLevel := Level.Warn
resolvers := Seq(
	"otto-bintray" at "https://dl.bintray.com/ottogroup/maven",
	"Sbt plugins" at "https://dl.bintray.com/sbt/sbt-plugin-releases"

)

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.8.2")
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.3")
addSbtPlugin("com.typesafe.sbt" %% "sbt-native-packager" % "1.3.2")