package evaluation.metrics.jmx

import evaluation.config.EvaluationConfig
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.slf4j.LoggerFactory

class JmxGCUtils(runTimes: DataFrame, sparkSession: SparkSession, evaluationConfig: EvaluationConfig) extends Serializable {
  val logger = LoggerFactory.getLogger(this.getClass)

  def compute(): DataFrame = {
    val runConfigColumns: Array[Column] = runTimes.select("runConfiguration.*").columns.map("runConfiguration." + _).map(col)
    val jmxMetrics = sparkSession.read.json(evaluationConfig.gcMetricsFrameworkPath)

    val gcMetrics = jmxMetrics.join(runTimes, jmxMetrics("time") < runTimes("endTime") && jmxMetrics("time") > runTimes("beginTime"))
      .drop("beginTime", "endTime")

    computeGCCollects(gcMetrics, runConfigColumns)
    computePctTimeonGC(gcMetrics, runConfigColumns)

    gcMetrics
  }


  /**
    * computes the distinct GC collects and the percentage change in the three memory pools: eden, survivor and old gen
    * @param gcMetrics
    * @param runConfigColumns
    */
  def computeGCCollects(gcMetrics: DataFrame, runConfigColumns: Array[Column]): Unit = {
    // we need to take the distinct collects because it is repeated for every second where there was no gc collect
    // but we still want to know when the collect happened so we do a groupby and then take the minimum time
    val gcCollects = gcMetrics.groupBy("runConfiguration", "containerName", "name", "lastGcDuration", "lastGcEndTime", "lastGcMemoryBefore", "lastGcMemoryAfter")
      .agg(min("time").as("time"))
      // pct change in the different memory pools of these collects
      .withColumn("pctChangeG1OldGen", (col("lastGcMemoryBefore.G1 Old Gen") - col("lastGcMemoryAfter.G1 Old Gen")) / col("lastGcMemoryBefore.G1 Old Gen"))
      .withColumn("pctChangeG1Survivor", (col("lastGcMemoryBefore.G1 Survivor Space") - col("lastGcMemoryAfter.G1 Survivor Space")) / col("lastGcMemoryBefore.G1 Survivor Space"))
      .withColumn("pctChangeG1Eden", (col("lastGcMemoryBefore.G1 Eden Space") - col("lastGcMemoryAfter.G1 Eden Space")) / col("lastGcMemoryBefore.G1 Eden Space"))

    gcCollects
      .withColumn("runConfigKey", concat_ws("-", runConfigColumns: _*))
      .select(col("runConfigKey"),
        col("runConfiguration.*"),
        col("containerName"),
        col("name"),
        col("time"),
        col("lastGcDuration"),
        col("lastGcEndTime"),
        col("pctChangeG1OldGen"),
        col("pctChangeG1Survivor"),
        col("pctChangeG1Eden"),
        col("lastGcMemoryBefore.G1 Old Gen").as("G1OldGenBefore"),
        col("lastGcMemoryBefore.G1 Survivor Space").as("G1SurvivorBefore"),
        col("lastGcMemoryBefore.G1 Eden Space").as("G1EdenBefore"),
        col("lastGcMemoryAfter.G1 Old Gen").as("G1OldGenAfter"),
        col("lastGcMemoryAfter.G1 Survivor Space").as("G1SurvivorAfter"),
        col("lastGcMemoryAfter.G1 Eden Space").as("G1EdenAfter"),
      )
      .sort("runConfigKey", "time")
      .coalesce(1)
      .write
      .partitionBy("runConfigKey")
      .option("header", "true")
      .csv(evaluationConfig.dataOutputPath("gc-collects"))
  }

  /**
    * computes the percentage of time that was spent on stop-the-world GC in a certain second
    * @param gcMetrics
    * @param runConfigColumns
    */
  def computePctTimeonGC(gcMetrics: DataFrame, runConfigColumns: Array[Column]): Unit = {
    val gcTimePerMemoryPool = gcMetrics
      .withColumn("timeDiff", col("time") - lag(col("time"), 1, null).over(Window.partitionBy("runConfiguration", "containerName", "name").orderBy("time")))

      .withColumn("previousCollectionTime", lag(col("collectionTime"), 1, 0).over(Window.partitionBy("runConfiguration", "containerName", "name").orderBy("time")))
      .withColumn("gcTimeOfThisSecond", (col("collectionTime") - col("previousCollectionTime")))

      .withColumn("previousCollectionCount", lag(col("collectionCount"), 1, 0).over(Window.partitionBy("runConfiguration", "containerName", "name").orderBy("time")))
      .withColumn("gcCountOfThisSecond", col("collectionCount") - col("previousCollectionCount"))

    gcTimePerMemoryPool
      .withColumn("runConfigKey", concat_ws("-", runConfigColumns: _*))
      .select("runConfigKey",
        "runConfiguration.*",
        "containerName",
        "name",
        "time",
        "gcTimeOfThisSecond",
        "gcCountOfThisSecond",
        "collectionTime",
        "collectionCount"
      )
      .sort("runConfigKey", "time")
      .coalesce(1)
      .write
      .partitionBy("runConfigKey")
      .option("header", "true")
      .csv(evaluationConfig.dataOutputPath("gc-time-per-memory-pool"))
  }

}
