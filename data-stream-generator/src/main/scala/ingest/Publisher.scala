package ingest

import java.util.concurrent.Executors

import com.codahale.metrics.Meter
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}

trait Publisher extends Serializable {
  implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8))
  val logger = LoggerFactory.getLogger(getClass)

  // log some stats every 5 seconds
  private val statsLogger = Future {
    @tailrec def logStats(): Unit = {
      logger.info(f"ingest stats - flow (count:${flowStats.getCount}, rate:${flowStats.getOneMinuteRate}%.1f), speed (count:${speedStats.getCount}, rate:${speedStats.getOneMinuteRate}%.1f)")
      Thread.sleep(5000)
      logStats()
    }

    logStats()
  }

  val flowStats = new Meter()
  val speedStats = new Meter()
  def publish(index: Int): Future[Unit]
}
