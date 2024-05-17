package com.github.fma.utils

import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.{Logger, LoggerFactory}
import slick.jdbc.PostgresProfile.api._

import scala.language.implicitConversions

class Utils {}

object Utils {
  val DEFAULT_BOOTSTRAP_SERVER = "127.0.0.1:9093"
  val FROM_TOPIC = "tweets-2"
  val TO_TOPIC = "retweets-2"
  val GROUP_ID = "nats-kafka-demo-1"
  val TIMEOUT_MILLS = 100
  val APPLICATION_ID = "streams-filter-tweets"

  val NATS_PORT = 4222
  val POSTGRES_PORT = 5432

  val DB_DRIVER = "db.driver"
  val DB_URL = "db.url"
  val DB_USERNAME = "db.username"
  val DB_PASSWORD = "db.password"

  val logger: Logger = LoggerFactory.getLogger(classOf[Utils])

  // var for ability to replace with mocks during tests
  private[fma] var getConfig: () => Config = () => ConfigFactory.load("application")
  private[fma] var getNatsPort: () => Int = () => NATS_PORT
  private[fma] var getBootstrapServers: () => String = () => DEFAULT_BOOTSTRAP_SERVER

  def getDB = {
    val config = getConfig()
    val url = config.getString(DB_URL)
    val username = config.getString(DB_USERNAME)
    val password = config.getString(DB_PASSWORD)
    val driver = config.getString(DB_PASSWORD)

    Database.forURL(url, username, password, null, driver)
  }
}
