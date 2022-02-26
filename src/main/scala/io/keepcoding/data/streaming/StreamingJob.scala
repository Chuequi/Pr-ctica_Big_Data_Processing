package io.keepcoding.data.streaming

import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.functions._


case class AntennaMessage(timestamp: Timestamp, id: String, metric: String, value: Long)

object StreamingJob {

  /*val df = spark.readStream.json(...)
val dfCounts = df.persist().unpersist().checkpoint().groupBy().count()
val query = dfCounts.writeStream.outputMode("complete").format("console").start()*/

  def main(args: Array[String]): Unit = {
    val KafkaServer = "34.176.134.201:9092"
    val JdbcURI = "jdbc:postgresql://35.224.229.95:5432/postresql"

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Streaming Job")
      .getOrCreate()

    import spark.implicits._

    val devicesSchema = StructType(Seq(
      StructField("timestamp", LongType, nullable = false),
      StructField("id", StringType, nullable = false),
      StructField("antenna_id", StringType, nullable = false),
      StructField("bytes", LongType, nullable = false),
      StructField("app", StringType, nullable = false)
    ))

    val deviceStream = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KafkaServer)
      .option("subscribe", "devices")
      .option("startingOffsets", "earliest")
      .load()
      .select(from_json($"value".cast(StringType), devicesSchema).as("json"))
      .select($"json.*")

    val storageStream = Future {
      deviceStream
        .select(
          $"id", $"antenna_id", $"bytes", $"app",
          year($"timestamp".cast(TimestampType)).as("year"),
          month($"timestamp".cast(TimestampType)).as("month"),
          dayofmonth($"timestamp".cast(TimestampType)).as("day"),
          hour($"timestamp".cast(TimestampType)).as("hour")
        )
        .writeStream
        .partitionBy("year", "month", "day", "hour")
        .format("parquet")
        .option("path", s"${StorageURI}/data")
        .option("checkpointLocation", s"${StorageURI}/checkpoint")
        .start()
        .awaitTermination()
    }

    val appStream = bytesTotalAgg(deviceStream, "app", JdbcURI)
    val userStream = bytesTotalAgg(deviceStream.withColumnRenamed("id", "user"), "user", JdbcURI)
    val antennaStream = bytesTotalAgg(deviceStream.withColumnRenamed("antenna_id", "antenna"), "antenna", JdbcURI)

    Await.result(Future.sequence(Seq(appStream, userStream, antennaStream, storageStream)), Duration.Inf)
  }

  def bytesTotalAgg(dataFrame: DataFrame, aggCol: String, jdbcURI: String): Future[Unit] = Future {
    import dataFrame.sparkSession.implicits._

    dataFrame
      .select($"timestamp".cast(TimestampType).as("timestamp"), col(aggCol), $"bytes")
      .withWatermark("timestamp", "1 minutes")
      .groupBy(window($"timestamp", "5 minutes"), col(aggCol))
      .agg(sum($"bytes").as("total_bytes"))
      .select(
        $"window.start".cast(TimestampType).as("timestamp"),
        col(aggCol).as("id"),
        $"total_bytes".as("value"),
        lit(s"${aggCol}_total_bytes").as("type")
      )
      .writeStream
      .foreachBatch((dataset: DataFrame, batchId: Long) =>
        dataset
          .write
          .mode(SaveMode.Append)
          .format("jdbc")
          .option("driver", "org.postgresql.Driver")
          .option("url", jdbcURI)
          .option("dbtable", "bytes")
          .option("user", "postresql")
          .option("password", "postresql")
          .save()
      )
      .start()
      .awaitTermination()
  }
}
