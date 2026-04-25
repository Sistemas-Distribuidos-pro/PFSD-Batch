package com.pfsd.batch.generator

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, LocalTime}
import scala.collection.mutable
import scala.util.Random

object SyntheticHistoryGenerator {

  final case class GeneratorConfig(
      ordersCount: Int,
      userCount: Int,
      alertRate: Double,
      outputBasePath: String,
      seed: Long,
      baseDate: LocalDate,
      daysSpan: Int,
      chunkSize: Int
  )

  private final case class OrderSeed(
      orderId: Long,
      userId: Long,
      total: BigDecimal,
      itemCount: Int,
      createdAt: LocalDateTime
  )

  private val StoredAtFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
  private val DateArgFormatter: DateTimeFormatter = DateTimeFormatter.ISO_LOCAL_DATE

  private val ReasonsCatalog: Vector[String] = Vector(
    "Monto alto (> $500.00)",
    "Cantidad inusual de \u00edtems",
    "Patr\u00f3n sospechoso"
  )

  def main(args: Array[String]): Unit = {
    val config = parseConfig(args)
    val random = new Random(config.seed)

    val ordersRoot = Paths.get(config.outputBasePath, "orders-bulk")
    val alertsRoot = Paths.get(config.outputBasePath, "alerts-bulk")

    resetDirectory(ordersRoot)
    resetDirectory(alertsRoot)

    val ordersByPartition = mutable.HashMap.empty[String, mutable.ArrayBuffer[String]]
    val alertsByPartition = mutable.HashMap.empty[String, mutable.ArrayBuffer[String]]

    var currentOrderId = 1L
    while (currentOrderId <= config.ordersCount.toLong) {
      val order = generateOrder(currentOrderId, config, random)

      val orderLine = orderJsonLine(order, random)
      addToPartition(ordersByPartition, partitionKey(order.createdAt), orderLine)

      if (random.nextDouble() < config.alertRate) {
        val alertLine = alertJsonLine(order, random)
        addToPartition(alertsByPartition, partitionKey(order.createdAt), alertLine)
      }

      currentOrderId += 1
    }

    writePartitionedChunks(ordersRoot, ordersByPartition.toMap, config.chunkSize)
    writePartitionedChunks(alertsRoot, alertsByPartition.toMap, config.chunkSize)

    val totalOrders = config.ordersCount
    val totalAlerts = alertsByPartition.values.map(_.size).sum

    println("Synthetic history generation completed.")
    println(s"Orders generated: $totalOrders")
    println(s"Alerts generated: $totalAlerts")
    println(s"Orders path: ${ordersRoot.toAbsolutePath}")
    println(s"Alerts path: ${alertsRoot.toAbsolutePath}")
  }

  private def generateOrder(orderId: Long, config: GeneratorConfig, random: Random): OrderSeed = {
    val rangeDays = math.max(1, config.daysSpan)
    val dayOffset = random.nextInt(rangeDays)
    val date = config.baseDate.minusDays(dayOffset.toLong)

    val hour = random.nextInt(24)
    val minute = random.nextInt(60)
    val second = random.nextInt(60)
    val nanos = random.nextInt(1000000000)

    val createdAt = LocalDateTime.of(date, LocalTime.of(hour, minute, second, nanos))

    val userId = (random.nextInt(math.max(1, config.userCount)) + 1).toLong

    val amountBase = 15.0 + random.nextDouble() * 285.0
    val amountTail = if (random.nextDouble() < 0.20) random.nextDouble() * 1000.0 else 0.0
    val total = BigDecimal(amountBase + amountTail).setScale(2, BigDecimal.RoundingMode.HALF_UP)

    val itemCount = {
      val r = random.nextDouble()
      if (r < 0.70) random.nextInt(3) + 1
      else if (r < 0.95) random.nextInt(5) + 4
      else random.nextInt(12) + 9
    }

    OrderSeed(
      orderId = orderId,
      userId = userId,
      total = total,
      itemCount = itemCount,
      createdAt = createdAt
    )
  }

  private def orderJsonLine(order: OrderSeed, random: Random): String = {
    val storedAt = order.createdAt.plusSeconds(random.nextInt(300).toLong)
    val createdAtArray = timestampArrayJson(order.createdAt)

    s"""{"eventType":"ORDER_CREATED","storedAt":"${storedAt.format(StoredAtFormatter)}","orderId":${order.orderId},"userId":${order.userId},"total":${formatAmount(order.total)},"itemCount":${order.itemCount},"createdAt":$createdAtArray}"""
  }

  private def alertJsonLine(order: OrderSeed, random: Random): String = {
    val detectedAt = order.createdAt.plusSeconds(random.nextInt(600).toLong)
    val storedAt = detectedAt.plusSeconds(random.nextInt(60).toLong)
    val reasons = selectReasons(order, random)

    val reasonsJson = reasons.map(r => "\"" + escapeJson(r) + "\"").mkString("[", ",", "]")
    val detectedAtArray = timestampArrayJson(detectedAt)

    s"""{"eventType":"ORDER_ALERT","storedAt":"${storedAt.format(StoredAtFormatter)}","orderId":${order.orderId},"userId":${order.userId},"total":${formatAmount(order.total)},"razones":$reasonsJson,"detectedAt":$detectedAtArray}"""
  }

  private def selectReasons(order: OrderSeed, random: Random): Vector[String] = {
    val selected = mutable.ArrayBuffer.empty[String]

    if (order.total > BigDecimal(500.0)) {
      selected += ReasonsCatalog(0)
    }
    if (order.itemCount >= 8) {
      selected += ReasonsCatalog(1)
    }
    if (selected.isEmpty || random.nextDouble() < 0.35) {
      selected += ReasonsCatalog(2)
    }

    selected.distinct.toVector
  }

  private def addToPartition(
      partitionMap: mutable.HashMap[String, mutable.ArrayBuffer[String]],
      partition: String,
      line: String
  ): Unit = {
    val bucket = partitionMap.getOrElseUpdate(partition, mutable.ArrayBuffer.empty[String])
    bucket += line
  }

  private def writePartitionedChunks(rootPath: Path, data: Map[String, mutable.ArrayBuffer[String]], chunkSize: Int): Unit = {
    data.foreach { case (partition, lines) =>
      val partitionPath = rootPath.resolve(partition)
      Files.createDirectories(partitionPath)

      lines.grouped(math.max(1, chunkSize)).zipWithIndex.foreach { case (chunk, idx) =>
        val fileName = f"part-${idx + 1}%05d.json"
        val content = chunk.mkString("", "\n", "\n")
        Files.write(
          partitionPath.resolve(fileName),
          content.getBytes(StandardCharsets.UTF_8),
          StandardOpenOption.CREATE,
          StandardOpenOption.TRUNCATE_EXISTING,
          StandardOpenOption.WRITE
        )
      }
    }
  }

  private def partitionKey(dateTime: LocalDateTime): String = {
    val year = dateTime.getYear
    val month = f"${dateTime.getMonthValue}%02d"
    val day = f"${dateTime.getDayOfMonth}%02d"
    s"year=$year/month=$month/day=$day"
  }

  private def timestampArrayJson(dateTime: LocalDateTime): String = {
    s"[${dateTime.getYear},${dateTime.getMonthValue},${dateTime.getDayOfMonth},${dateTime.getHour},${dateTime.getMinute},${dateTime.getSecond},${dateTime.getNano}]"
  }

  private def formatAmount(amount: BigDecimal): String = {
    amount.setScale(2, BigDecimal.RoundingMode.HALF_UP).toString()
  }

  private def parseConfig(args: Array[String]): GeneratorConfig = {
    val argMap = args
      .flatMap { arg =>
        if (arg.startsWith("--") && arg.contains("=")) {
          val parts = arg.drop(2).split("=", 2)
          Some(parts(0) -> parts(1))
        } else {
          None
        }
      }
      .toMap

    val ordersCount = parseIntArg(argMap, "ordersCount", 10000)
    val userCount = parseIntArg(argMap, "userCount", 100)
    val alertRate = parseDoubleArg(argMap, "alertRate", 0.05)
    val outputBasePath = argMap.getOrElse("outputBasePath", "generated-data")
    val seed = parseLongArg(argMap, "seed", 42L)
    val baseDate = argMap
      .get("baseDate")
      .map(v => LocalDate.parse(v, DateArgFormatter))
      .getOrElse(LocalDate.now())
    val daysSpan = parseIntArg(argMap, "daysSpan", 30)
    val chunkSize = parseIntArg(argMap, "chunkSize", 5000)

    GeneratorConfig(
      ordersCount = math.max(1, ordersCount),
      userCount = math.max(1, userCount),
      alertRate = math.max(0.0, math.min(1.0, alertRate)),
      outputBasePath = outputBasePath,
      seed = seed,
      baseDate = baseDate,
      daysSpan = math.max(1, daysSpan),
      chunkSize = math.max(1, chunkSize)
    )
  }

  private def parseIntArg(args: Map[String, String], key: String, defaultValue: Int): Int = {
    args.get(key).flatMap(v => scala.util.Try(v.toInt).toOption).getOrElse(defaultValue)
  }

  private def parseLongArg(args: Map[String, String], key: String, defaultValue: Long): Long = {
    args.get(key).flatMap(v => scala.util.Try(v.toLong).toOption).getOrElse(defaultValue)
  }

  private def parseDoubleArg(args: Map[String, String], key: String, defaultValue: Double): Double = {
    args
      .get(key)
      .flatMap(v => scala.util.Try(v.toDouble).toOption)
      .map(v => BigDecimal(v).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble)
      .getOrElse(defaultValue)
  }

  private def resetDirectory(path: Path): Unit = {
    if (Files.exists(path)) {
      val allPaths = Files.walk(path).toArray.map(_.asInstanceOf[Path]).sortBy(_.toString.length).reverse
      allPaths.foreach(Files.deleteIfExists)
    }
    Files.createDirectories(path)
  }

  private def escapeJson(value: String): String = {
    value
      .replace("\\", "\\\\")
      .replace("\"", "\\\"")
  }
}
