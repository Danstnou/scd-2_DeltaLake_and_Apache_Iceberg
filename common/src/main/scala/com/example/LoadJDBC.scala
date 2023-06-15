package com.example

import org.apache.spark.sql.{DataFrame, SparkSession}

trait LoadJDBC {

  def loadJDBC(query: String)(implicit spark: SparkSession, config: SCD2Config): DataFrame = {
    println(s"Загружаем через jdbc: $query")

    spark.read.format("jdbc")
      .option("url", config.url)
      .option("driver", config.driver)
      .option("dbtable", s"($query) t")
      .option("user", config.user)
      .option("password", config.password)
      .load()
  }

}
