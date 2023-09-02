package ru.otus

import org.slf4j.LoggerFactory
import ch.qos.logback.classic.{Level, Logger}
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import com.typesafe.config.ConfigFactory
import org.apache.kafka.common.serialization.{IntegerDeserializer, StringDeserializer}

object  ConsumerConf {
  private val config = ConfigFactory.load()
  private val consumerConfig = config.getConfig("akka.kafka.consumer")
  private val consumerSettings = ConsumerSettings(consumerConfig, new StringDeserializer, new IntegerDeserializer)

  LoggerFactory
    .getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME)
    .asInstanceOf[Logger]
    .setLevel(Level.ERROR)

  val source = Consumer.plainSource(consumerSettings, Subscriptions.topics("test")).
    map((r) => r.value().intValue())
}