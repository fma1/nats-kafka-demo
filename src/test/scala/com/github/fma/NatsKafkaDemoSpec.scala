package com.github.fma

import org.scalatest._
import org.scalatest.flatspec._
import org.scalatest.matchers._
import org.testcontainers.containers.{GenericContainer, KafkaContainer, PostgreSQLContainer}
import org.testcontainers.utility.DockerImageName

import scala.collection.mutable

class NatsKafkaDemoSpec extends AnyFlatSpec with should.Matchers with BeforeAndAfter {
  // final DockerImageName REDIS_IMAGE = DockerImageName.parse("redis:3.0.2")
  val NATS_IMAGE: DockerImageName = DockerImageName.parse("nats:2.2.2")
  val KAFKA_IMAGE: DockerImageName = DockerImageName.parse("confluentinc/cp-kafka:5.5.4")
  val POSTGRES_IMAGE: DockerImageName = DockerImageName.parse("postgres:9.6.21")

  var natsContainer: GenericContainer[_] = _
  var kafkaContainer: KafkaContainer = _
  var postgresContainer: PostgreSQLContainer[_] = _

  // TODO: Change BOOTSTRAP_SERVER so it can be provided by test
  // TODO: Add NATS_SERVER so it can be provided by test
  // TODO: Change getDB so Postgres URL can be provided by test

  before {
    /*
    natsContainer = new GenericContainer(NATS_IMAGE)
    kafkaContainer = new KafkaContainer(KAFKA_IMAGE)
     */
    postgresContainer = new PostgreSQLContainer1(POSTGRES_IMAGE)
      .withDatabaseName("twitter")
      .withUsername("postgres")
      .withPassword("root")
    postgresContainer.start()
  }

  after {
    postgresContainer.stop()
  }

  "Postgres container" should "be running" in {
    assert(postgresContainer.isRunning)
  }


  class PostgreSQLContainer1(dockerImageName: DockerImageName)
    extends PostgreSQLContainer[PostgreSQLContainer1](dockerImageName)
}