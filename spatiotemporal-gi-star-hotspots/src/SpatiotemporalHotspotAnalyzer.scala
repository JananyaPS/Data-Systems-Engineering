package spatial.analytics

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

object SpatiotemporalHotspotAnalyzer {

  private def configureLogging(): Unit = {
    Seq("org.spark_project", "org.apache", "akka", "com").foreach { name =>
      Logger.getLogger(name).setLevel(Level.WARN)
    }
  }

  /**
   * Computes Gi* scores over a space-time grid and returns hotspot-ranked cells.
   *
   * @param spark SparkSession
   * @param pointPath semicolon-delimited input; pickup point in _c5 and pickup time in _c1
   */
  def run(spark: SparkSession, pointPath: String): DataFrame = {
    configureLogging()
    import spark.implicits._

    // 1) Load data
    val trips = spark.read
      .format("com.databricks.spark.csv")
      .option("delimiter", ";")
      .option("header", "false")
      .load(pointPath)

    trips.createOrReplaceTempView("events")

    // 2) Compute integer cell coordinates (x,y,z)
    spark.udf.register("CalcX", (p: String) => SpatiotemporalGridUtils.calculateCoordinate(p, 0))
    spark.udf.register("CalcY", (p: String) => SpatiotemporalGridUtils.calculateCoordinate(p, 1))
    spark.udf.register("CalcZ", (t: String) => SpatiotemporalGridUtils.calculateCoordinate(t, 2))

    val cellsDf = spark.sql(
      """
        |SELECT
        |  CalcX(_c5) AS x,
        |  CalcY(_c5) AS y,
        |  CalcZ(_c1) AS z
        |FROM events
      """.stripMargin
    )

    // 3) Study area bounds
    val minX = -74.50 / SpatiotemporalGridUtils.coordinateStep
    val maxX = -73.70 / SpatiotemporalGridUtils.coordinateStep
    val minY =  40.50 / SpatiotemporalGridUtils.coordinateStep
    val maxY =  40.90 / SpatiotemporalGridUtils.coordinateStep
    val minZ = 1
    val maxZ = 31

    val minXi = math.floor(minX).toInt
    val maxXi = math.floor(maxX).toInt
    val minYi = math.floor(minY).toInt
    val maxYi = math.floor(maxY).toInt
    val minZi = minZ
    val maxZi = maxZ

    val numCells = (maxXi - minXi + 1).toLong *
      (maxYi - minYi + 1).toLong *
      (maxZi - minZi + 1).toLong

    val N = numCells.toDouble

    // 4) Count events per non-empty cell (within bounds)
    val nonEmpty = cellsDf
      .filter(col("x").between(minXi, maxXi) &&
        col("y").between(minYi, maxYi) &&
        col("z").between(minZi, maxZi))
      .groupBy("x", "y", "z")
      .agg(count(lit(1)).as("cnt"))
      .cache()

    // 5) Global mean + std (empty cells contribute 0 via N)
    val statsRow = nonEmpty
      .agg(
        sum(col("cnt").cast(DoubleType)).as("sumX"),
        sum(pow(col("cnt").cast(DoubleType), 2)).as("sumX2")
      )
      .first()

    val sumX = Option(statsRow.getAs[java.lang.Double]("sumX")).map(_.doubleValue()).getOrElse(0.0)
    val sumX2 = Option(statsRow.getAs[java.lang.Double]("sumX2")).map(_.doubleValue()).getOrElse(0.0)

    val mean = sumX / N
    val std = math.sqrt((sumX2 / N) - (mean * mean))

    // 6) 27-neighborhood offsets
    val offsets = (-1 to 1).flatMap(dx => (-1 to 1).flatMap(dy => (-1 to 1).map(dz => (dx, dy, dz))))
    val offsetsDf = offsets.toDF("dx", "dy", "dz")

    // 7) Neighbor aggregation (sumW, W)
    val neighAgg = nonEmpty.as("c")
      .crossJoin(offsetsDf)
      .withColumn("nx", col("c.x") + col("dx"))
      .withColumn("ny", col("c.y") + col("dy"))
      .withColumn("nz", col("c.z") + col("dz"))
      .filter(col("nx").between(minXi, maxXi) &&
        col("ny").between(minYi, maxYi) &&
        col("nz").between(minZi, maxZi))
      .join(
        nonEmpty.as("n"),
        col("n.x") === col("nx") && col("n.y") === col("ny") && col("n.z") === col("nz"),
        "left_outer"
      )
      .groupBy(col("c.x").as("x"), col("c.y").as("y"), col("c.z").as("z"))
      .agg(
        sum(coalesce(col("n.cnt").cast(DoubleType), lit(0.0))).as("sumW"),
        count(lit(1)).as("W")
      )

    // 8) Gi* score
    val giDf = neighAgg.withColumn(
      "gi",
      (col("sumW") - lit(mean) * col("W").cast(DoubleType)) /
        (lit(std) * sqrt((lit(N) * col("W").cast(DoubleType) - pow(col("W").cast(DoubleType), 2.0)) / (lit(N) - lit(1.0))))
    )

    // 9) Sort by Gi* desc; return x,y,z
    giDf
      .orderBy(col("gi").desc, col("x"), col("y"), col("z"))
      .select("x", "y", "z")
  }
}
