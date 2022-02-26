package io.keepcoding.data.batch

import java.time.OffsetDateTime
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType

object BatchJob {

  def main(args: Array[String]): Unit = {
    val JdbcURI = "jdbc:postgresql://35.224.229.95:5432/postresql"
    val argsTime = "2022-02-26T15:32:06Z"
    val filterDate = OffsetDateTime.parse(argsTime)

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Batch Job")
      .getOrCreate()

    import spark.implicits._

    val userMetadataDF = spark
      .read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", JdbcURI)
      .option("dbtable", "user_metadata")
      .option("user", "postresql")
      .option("password", "postresql")
      .load()

    val devicesDataDF = spark
      .read
      .format("parquet")
      .load()
      .filter(
        $"year" === filterDate.getYear &&
          $"month" === filterDate.getMonthValue &&
          $"day" === filterDate.getDayOfMonth &&
          $"hour" === filterDate.getHour
      )
      .persist()

    val userTotalBytesDF = computeBytes(devicesDataDF, "id", "user_bytes_total", filterDate).persist()

    val userQuotaLimitDF = userTotalBytesDF.as("user").select($"id", $"value")
      .join(
        userMetadataDF.select($"id", $"email", $"quota").as("metadata"),
        $"user.id" === $"metadata.id" && $"user.value" > $"metadata.quota"
      )
      .select($"metadata.email", $"user.value".as("usage"), $"metadata.quota", lit(filterDate.toEpochSecond).cast(TimestampType).as("timestamp"))

    Await.result(
      Future.sequence(Seq(
        writeToJdbc(computeBytes(devicesDataDF, "antenna_id", "antenna_bytes_total", filterDate), JdbcURI, "bytes_hourly"),
        writeToJdbc(computeBytes(devicesDataDF, "app", "app_bytes_total", filterDate), JdbcURI, "bytes_hourly"),
        writeToJdbc(userTotalBytesDF, JdbcURI, "bytes_hourly"),
        writeToJdbc(userQuotaLimitDF, JdbcURI, "user_quota_limit")
      )), Duration.Inf
    )
  }

  def computeBytes(devicesDataDF: DataFrame, colName: String, metricName: String, filterDate: OffsetDateTime): DataFrame = {
    import devicesDataDF.sparkSession.implicits._

    devicesDataDF
      .select(col(colName).as("id"), $"bytes")
      .groupBy($"id")
      .agg(sum($"bytes").as("value"))
      .withColumn("type", lit(metricName))
      .withColumn("timestamp", lit(filterDate.toEpochSecond).cast(TimestampType))
  }

  def writeToJdbc(devicesDataDF: DataFrame, jdbcURI: String, tableName: String): Future[Unit] = Future {
    devicesDataDF
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", jdbcURI)
      .option("dbtable", tableName)
      .option("user", "postresql")
      .option("password", "postresql")
      .save()
  }
}
