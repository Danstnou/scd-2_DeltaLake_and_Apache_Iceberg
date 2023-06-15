package com.example

import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

class SCD2DeltaLakeTable(tableName: String, keyCols: Seq[String],
                         trackedValuesCols: Seq[String], updateCols: Map[String, String])(implicit config: SCD2Config)
  extends SCD2Table(tableName, keyCols, trackedValuesCols) {

  override def scd2Alg(updated: DataFrame): Unit = {
    val deltaTable = DeltaTable.forName(spark, tableName)
    deltaTable.toDF.createOrReplaceTempView("t")
    updated.createOrReplaceTempView("u")

    // находим новые значения
    val newValExistingRows =
      spark.sql(
        s"SELECT u.* FROM u INNER JOIN t ON $hashUkeyCols = $hashTkeyCols " +
          s"WHERE t.effective_to is null AND $hashUtrackedCols <> $hashTtrackedCols"
      )
        .withColumn(keyCol, lit(null) cast StringType)

    val updatesWithMergeKey =
      updated
        .withColumn(keyCol, hash(keyCols.map(col): _*))

    deltaTable.as("t").merge(
      source = (newValExistingRows unionByName updatesWithMergeKey) as "u",
      condition = s"$hashTkeyCols = u.$keyCol "
    )
      .whenMatched(s"t.effective_to is null AND $hashTtrackedCols <> $hashUtrackedCols")
      .updateExpr(updateCols.mapValues("u." + _))
      .whenNotMatched
      .insertAll()
      .execute()
  }

  override def createTable(): Unit = {
    loadJDBC("select * from spotify limit 0")
      .withEffectiveCols()
      .write.format("delta").mode("overwrite").saveAsTable(tableName)
  }

  override implicit val spark: SparkSession = {
    val sparkSession =
      SparkSession.builder()
        .master("local[*]")
        .enableHiveSupport()
        .appName("SCD2WithDeltaLake")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", false)
        .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")
    sparkSession
  }

}