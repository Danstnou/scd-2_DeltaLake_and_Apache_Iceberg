import com.example._
import org.apache.spark.sql.SparkSession

import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

object Main extends App with LoadJDBC {
  implicit val config = SCD2Config("jdbc:postgresql://localhost:5432/productDb", "org.postgresql.Driver", "admin", "admin1234")

  private val (dlTableName, icebergTableName) = ("spotify_dl.rating_delta_lake", s"local.spotify_ib.rating")

  private val (keyCols, trackedValuesCols) = (Seq("artist", "title", "country"), Seq("position"))
  private val updateCols = Map("effective_to" -> "date")

  private def createSCD2DL(): SCD2Table = {
    val scd2DlTable = new SCD2DeltaLakeTable(dlTableName, keyCols, trackedValuesCols, updateCols)
    scd2DlTable.spark.sql("create database if not exists spotify_dl")
    scd2DlTable
  }

  private def createSCD2Iceberg(): SCD2Table =
    new SCD2IcebergTable(icebergTableName, keyCols, trackedValuesCols, updateCols)

  private def simulatedDailyLoading(scd2Table: SCD2Table): Unit = {
    implicit val spark: SparkSession = scd2Table.spark
    val dates = loadJDBC(s"select distinct date from spotify order by date").collect().map(_.getTimestamp(0)).toSeq

    scd2Table.createTable()

    val long = dates
      .zipWithIndex
      .foldLeft((dates.size - 1).toLong) { case (lastIndex, (timestamp, i)) =>
        val sourceDF = loadJDBC(s"select * from spotify where date = '$timestamp'")

        if (i == lastIndex) {
          val beginTime = LocalDateTime.now()
          scd2Table.scd2(sourceDF)
          val endTime = LocalDateTime.now()

          ChronoUnit.SECONDS.between(beginTime, endTime)
        }
        else {
          scd2Table.scd2(sourceDF)
          lastIndex
        }
      }

    println("Продолжительность на последней дате: " + long + " сек")
  }

  val where = "artist='Eminem' and title='Godzilla' and country='Global'"

  def executeScd2(scd2Table: SCD2Table, storageName: String): Unit = {
    println(s"Симуляция ежедневной загрузки с scd2 через $storageName")
    simulatedDailyLoading(scd2Table)

    println(s"История изменений таблицы $storageName для ключа: $where")
    scd2Table.spark.table(scd2Table.tableName).where(where).orderBy("date").show()
    scd2Table.spark.close()
  }

  executeScd2(createSCD2Iceberg(), "Iceberg")
  executeScd2(createSCD2DL(), "Delta Lake")
}