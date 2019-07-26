package ingest

import com.typesafe.config.{Config, ConfigFactory}

object ConfigUtils extends Serializable {
  val configProperties: Config = ConfigFactory.load("resources.conf")

  val MODE = sys.env("MODE")

  val flowTopic = sys.env("FLOWTOPIC")
  val speedTopic = sys.env("SPEEDTOPIC")

}
