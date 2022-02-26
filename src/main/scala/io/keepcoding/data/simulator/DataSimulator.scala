package io.keepcoding.data.simulator

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.io.Source

import io.circe.parser._
import io.circe.syntax.EncoderOps
import scala.util.Random
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

object DataSimulator {
  val FrequencyTimeMs = 60000
  val Apps = Seq("SKYPE", "TELEGRAM", "FACETIME", "FACEBOOK", "WHATSAPP")
  val random = new Random

  def generateData(kafkaProducer: KafkaProducer[String, String], file: String, topic: String): Future[Unit] = Future {
    val lines = Source.fromInputStream(getClass.getClassLoader.getResourceAsStream(file)).getLines

    lines.foreach {
      case "---" =>
        println(topic, "Time to sleep!")
        Thread.sleep(FrequencyTimeMs)
      case line =>
        parse(line) match {
          case Right(json) =>

            val message = {
              if (topic == "devices") {
                json
                  .deepMerge(Map("app" -> Apps(random.nextInt(Apps.size - 1))).asJson)
                  .deepMerge(Map("bytes" -> random.nextInt(10000), "timestamp" -> (System.currentTimeMillis() / 1000).toInt).asJson)
                  .toString()
              } else json.toString()
            }.replaceAll("\\s", "")

            println(topic, s"Sending: $message")
            kafkaProducer.send(new ProducerRecord[String, String](topic, message))
          case Left(error) =>
            println(topic, error.toString)
        }
    }
  }

  def main(args: Array[String]): Unit = {
    val properties = new Properties
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, args(0))
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    println(s"Connecting to KafkaBroker with this properties $properties")
    val kafkaProducer = new KafkaProducer[String, String](properties)

    Await.result(Future.sequence(
      Seq(
        generateData(kafkaProducer, "devices.json", "devices"),
        generateData(kafkaProducer, "antenna_telemetry.json", "antenna_telemetry")
      )
    ), Duration.Inf)

    kafkaProducer.close()
  }
}
