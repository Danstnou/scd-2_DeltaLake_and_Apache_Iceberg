package com.example

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

abstract class SCD2Table(val tableName: String, keyCols: Seq[String], trackedValuesCols: Seq[String]) extends LoadJDBC {
  val keyCol = "hash_key"

  val (hashUkeyCols, hashTkeyCols) =
    (createHashExpr(keyCols.map("u." + _)), createHashExpr(keyCols.map("t." + _)))

  val (hashUtrackedCols, hashTtrackedCols) =
    (createHashExpr(trackedValuesCols.map("u." + _)), createHashExpr(trackedValuesCols.map("t." + _)))

  def createHashExpr(columns: Seq[String]): String = hash(columns.map(col): _*).toString()

  def scd2(updated: DataFrame) = scd2Alg(updated.dropDuplicates().withEffectiveCols())

  implicit class RichDataFrame(df: DataFrame) {
    def withEffectiveCols(): DataFrame =
      df.selectExpr("*", "date as effective_from", "to_timestamp(null) as effective_to")
  }

  implicit val spark: SparkSession

  def scd2Alg(updated: DataFrame): Unit

  def createTable(): Unit
}