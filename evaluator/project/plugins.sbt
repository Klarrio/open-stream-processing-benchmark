logLevel := Level.Warn
resolvers := Seq(
	"otto-bintray" at "https://dl.bintray.com/ottogroup/maven",
	"Sbt plugins" at "https://dl.bintray.com/sbt/sbt-plugin-releases"
)

addSbtPlugin("com.typesafe.sbt" %% "sbt-native-packager" % "1.3.2")
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.3")
addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.7.0")
