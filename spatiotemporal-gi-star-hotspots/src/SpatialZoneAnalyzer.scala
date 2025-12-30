package spatial.analytics

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object SpatialZoneAnalyzer {

  private def configureLogging(): Unit = {
    Seq("org.spark_project", "org.apache", "akka", "com").foreach { name =>
      Logger.getLogger(name).setLevel(Level.WARN)
    }
  }

  /**
   * Counts points contained in each rectangle (spatial containment join).
   *
   * @param spark SparkSession
   * @param pointPath input dataset path (semicolon-delimited; point in column _c5)
   * @param rectanglePath rectangle dataset path (tab-delimited; rectangle in column _c0)
   */
  def run(spark: SparkSession, pointPath: String, rectanglePath: String): DataFrame = {
    configureLogging()

    val rawPoints = spark.read.format("com.databricks.spark.csv")
      .option("delimiter", ";")
      .option("header", "false")
      .load(pointPath)

    // Clean "(x,y)" -> "x,y"
    val stripParens = udf { s: String =>
      if (s == null) null else s.replace("(", "").replace(")", "")
    }

    val pointStrings = rawPoints
      .select(stripParens(col("_c5")).as("pointString"))
      .where(col("pointString").isNotNull)

    val rectangles = spark.read.format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header", "false")
      .load(rectanglePath)

    // Register containment predicate UDF
    spark.udf.register(
      "ST_Contains",
      (rect: String, pt: String) => SpatialZoneFunctions.contains(rect, pt)
    )

    rectangles.createOrReplaceTempView("rectangles")
    pointStrings.createOrReplaceTempView("points")

    spark.sql(
      """
        |SELECT r._c0 AS rectangle, COUNT(*) AS count
        |FROM rectangles r, points p
        |WHERE ST_Contains(r._c0, p.pointString)
        |GROUP BY r._c0
        |ORDER BY r._c0 ASC
      """.stripMargin
    )
  }
}
