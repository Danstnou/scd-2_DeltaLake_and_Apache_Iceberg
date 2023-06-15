package com.example

import org.apache.spark.sql.{DataFrame, SparkSession}

class SCD2IcebergTable(tableName: String, keyCols: Seq[String],
                       trackedValuesCols: Seq[String], updateCols: Map[String, String])(implicit config: SCD2Config)
  extends SCD2Table(tableName, keyCols, trackedValuesCols) {

  override def scd2Alg(updated: DataFrame): Unit = {
    updated.createOrReplaceTempView("u")

    val newValExistingRowsQuery =
      s"SELECT u.*, (cast (null as string)) as $keyCol FROM " +
        s"u INNER JOIN $tableName t ON $hashUkeyCols = $hashTkeyCols " +
        s"WHERE t.effective_to is null AND $hashUtrackedCols <> $hashTtrackedCols"

    val updatesWithMergeKeyQuery = s"select *, ${createHashExpr(keyCols)} as $keyCol from u"

    val updateExprStr = updateCols.map { case (k, v) => s"t.$k = u.$v" }.mkString(",")
    spark.sql(
      s"MERGE INTO $tableName t " +
        s"USING (($newValExistingRowsQuery) union all ($updatesWithMergeKeyQuery)) u " +
        s"on $hashTkeyCols = u.$keyCol " +
        s"WHEN MATCHED AND t.effective_to is null AND $hashTtrackedCols <> $hashUtrackedCols " +
        s"THEN UPDATE SET $updateExprStr WHEN NOT MATCHED THEN INSERT *"
    )
  }

  override def createTable(): Unit = {
    loadJDBC("select * from spotify limit 0")
      .withEffectiveCols()
      .writeTo(tableName).createOrReplace()
  }

  override implicit val spark: SparkSession = {
    val sparkSession =
      SparkSession.builder()
        .master("local[*]")
        .enableHiveSupport()
        .appName("scd2-Iceberg")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", "spark-warehouse")
        .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")
    sparkSession
  }

}