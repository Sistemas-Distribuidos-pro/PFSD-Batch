package com.pfsd.batch

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.Properties

object BatchAnalyticsJob {

  final case class JobConfig(
      ordersPath: String,
      alertsPath: String,
      outputPath: String,
      topN: Int
  )

  def main(args: Array[String]): Unit = {
    val config = loadConfig(args)

    val spark = SparkSession
      .builder()
      .appName("PFSD Batch Analytics")
      .master("local[*]")
      .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try {
      val ordersDf = readOrders(spark, config.ordersPath)
      val alertsDf = readAlerts(spark, config.alertsPath)

      val totalOrders = ordersDf.count()
      val totalAlerts = alertsDf.count()

      val ordersWithAlerts = alertsDf.select("orderId").distinct().count()
      val pctOrdersWithAlerts =
        if (totalOrders == 0L) 0.0 else (ordersWithAlerts.toDouble / totalOrders.toDouble) * 100.0

      val avgOrderTotal = scalarDouble(ordersDf.select(avg(col("total")).alias("avgOrderTotal")), "avgOrderTotal")
      val avgItemCount = scalarDouble(ordersDf.select(avg(col("itemCount")).alias("avgItemCount")), "avgItemCount")

      val topUsersByAlerts = alertsDf
        .groupBy("userId")
        .agg(count(lit(1)).alias("alertCount"))
        .orderBy(col("alertCount").desc, col("userId").asc)
        .limit(config.topN)

      val highestOrderTotals = ordersDf
        .select("orderId", "userId", "total", "createdAt")
        .orderBy(col("total").desc, col("orderId").asc)
        .limit(config.topN)

      val alertReasonFrequency = alertsDf
        .withColumn("razon", explode_outer(col("razones")))
        .filter(col("razon").isNotNull)
        .groupBy("razon")
        .agg(count(lit(1)).alias("count"))
        .orderBy(col("count").desc, col("razon").asc)

      val summaryDf = spark.createDataFrame(
        Seq(
          (
            totalOrders,
            totalAlerts,
            BigDecimal(pctOrdersWithAlerts).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble,
            BigDecimal(avgOrderTotal).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble,
            BigDecimal(avgItemCount).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
          )
        )
      ).toDF(
        "totalOrders",
        "totalAlerts",
        "percentageOrdersWithAlerts",
        "averageOrderTotal",
        "averageItemCount"
      )

      writeOutputs(summaryDf, topUsersByAlerts, highestOrderTotals, alertReasonFrequency, config.outputPath)
      printSummary(summaryDf, topUsersByAlerts, highestOrderTotals, alertReasonFrequency)
    } finally {
      spark.stop()
    }
  }

  private def readOrders(spark: SparkSession, ordersPath: String): DataFrame = {
    val ordersSchema = StructType(
      Seq(
        StructField("eventType", StringType, nullable = true),
        StructField("storedAt", TimestampType, nullable = true),
        StructField("orderId", LongType, nullable = true),
        StructField("userId", LongType, nullable = true),
        StructField("total", DecimalType(18, 2), nullable = true),
        StructField("itemCount", IntegerType, nullable = true),
        StructField("createdAt", TimestampType, nullable = true)
      )
    )

    spark.read
      .schema(ordersSchema)
      .json(ordersPath)
      .select("eventType", "storedAt", "orderId", "userId", "total", "itemCount", "createdAt")
  }

  private def readAlerts(spark: SparkSession, alertsPath: String): DataFrame = {
    val alertsSchema = StructType(
      Seq(
        StructField("eventType", StringType, nullable = true),
        StructField("storedAt", TimestampType, nullable = true),
        StructField("orderId", LongType, nullable = true),
        StructField("userId", LongType, nullable = true),
        StructField("total", DecimalType(18, 2), nullable = true),
        StructField("razones", ArrayType(StringType), nullable = true),
        StructField("detectedAt", TimestampType, nullable = true)
      )
    )

    spark.read
      .schema(alertsSchema)
      .json(alertsPath)
      .select("eventType", "storedAt", "orderId", "userId", "total", "razones", "detectedAt")
  }

  private def writeOutputs(
      summaryDf: DataFrame,
      topUsersByAlerts: DataFrame,
      highestOrderTotals: DataFrame,
      alertReasonFrequency: DataFrame,
      outputPath: String
  ): Unit = {
    summaryDf.coalesce(1).write.mode(SaveMode.Overwrite).json(s"$outputPath/batch-report-json")
    summaryDf.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv(s"$outputPath/batch-report-csv")

    topUsersByAlerts.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv(s"$outputPath/top-users-by-alerts")
    highestOrderTotals.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv(s"$outputPath/highest-order-totals")
    alertReasonFrequency.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv(s"$outputPath/alert-reason-frequency")
  }

  private def printSummary(
      summaryDf: DataFrame,
      topUsersByAlerts: DataFrame,
      highestOrderTotals: DataFrame,
      alertReasonFrequency: DataFrame
  ): Unit = {
    println("==== PFSD Batch Analytics Summary ====")
    summaryDf.show(truncate = false)

    println("==== Top Users By Alerts ====")
    topUsersByAlerts.show(truncate = false)

    println("==== Highest Order Totals ====")
    highestOrderTotals.show(truncate = false)

    println("==== Alert Reason Frequency (razones) ====")
    alertReasonFrequency.show(truncate = false)
  }

  private def scalarDouble(df: DataFrame, field: String): Double = {
    Option(df.first().getAs[Any](field))
      .map {
        case d: java.lang.Double     => d.doubleValue()
        case f: java.lang.Float      => f.doubleValue()
        case l: java.lang.Long       => l.doubleValue()
        case i: java.lang.Integer    => i.doubleValue()
        case bd: java.math.BigDecimal => bd.doubleValue()
        case n: java.lang.Number     => n.doubleValue()
        case _                       => 0.0
      }
      .getOrElse(0.0)
  }

  private def loadConfig(args: Array[String]): JobConfig = {
    val defaults = loadDefaults()
    val argMap = parseArgs(args)

    val ordersPath =
      argMap
        .get("ordersPath")
        .orElse(sys.env.get("PFSD_ORDERS_PATH"))
        .orElse(Option(defaults.getProperty("orders.path")))
        .getOrElse("s3a://pfsd-order-history/orders/")

    val alertsPath =
      argMap
        .get("alertsPath")
        .orElse(sys.env.get("PFSD_ALERTS_PATH"))
        .orElse(Option(defaults.getProperty("alerts.path")))
        .getOrElse("s3a://pfsd-order-history/alerts/")

    val outputPath =
      argMap
        .get("outputPath")
        .orElse(sys.env.get("PFSD_OUTPUT_PATH"))
        .orElse(Option(defaults.getProperty("output.path")))
        .getOrElse("output")

    val topN =
      argMap
        .get("topN")
        .orElse(sys.env.get("PFSD_TOP_N"))
        .orElse(Option(defaults.getProperty("top.n")))
        .flatMap(v => scala.util.Try(v.toInt).toOption)
        .filter(_ > 0)
        .getOrElse(10)

    JobConfig(
      ordersPath = ordersPath,
      alertsPath = alertsPath,
      outputPath = outputPath,
      topN = topN
    )
  }

  private def loadDefaults(): Properties = {
    val props = new Properties()
    val resource = Option(getClass.getClassLoader.getResourceAsStream("batch-defaults.properties"))
    resource.foreach { stream =>
      try props.load(stream)
      finally stream.close()
    }
    props
  }

  private def parseArgs(args: Array[String]): Map[String, String] = {
    args
      .flatMap { arg =>
        if (arg.startsWith("--") && arg.contains("=")) {
          val parts = arg.drop(2).split("=", 2)
          Some(parts(0) -> parts(1))
        } else {
          None
        }
      }
      .toMap
