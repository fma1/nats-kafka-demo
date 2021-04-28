package com.github.fma

import com.github.fma.Utils._
import com.typesafe.config.Config
import org.scalatest._
import org.scalatest.flatspec._
import org.scalatest.matchers._
import org.slf4j.{Logger, LoggerFactory}
import org.specs2.mock.Mockito
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy
import org.testcontainers.containers.{GenericContainer, KafkaContainer, PostgreSQLContainer}
import org.testcontainers.utility.DockerImageName
import slick.jdbc.PostgresProfile.api._

import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

class NatsKafkaDemoSpec extends AnyFlatSpec with should.Matchers with BeforeAndAfter with Mockito {
  val logger: Logger = LoggerFactory.getLogger(classOf[NatsKafkaDemoSpec])

  /*
   * NOTE: I changed the test database name, username and password
   * to show the example of where the test docker database would be different
   * from the actual database
   */
  val TEST_DB_NAME = "twitter1"
  val TEST_DB_USERNAME = "postgres1"
  val TEST_DB_PASSWORD = "root1"

  val TWEETS_COUNT = 196

  val NATS_IMAGE: DockerImageName = DockerImageName.parse("nats:2.2.2")
  val KAFKA_IMAGE: DockerImageName = DockerImageName.parse("confluentinc/cp-kafka:5.4.3")
  val POSTGRES_IMAGE: DockerImageName = DockerImageName.parse("postgres:9.6.21")

  var natsContainer: GenericContainer1 = _
  var kafkaContainer: KafkaContainer = _
  var postgresContainer: PostgreSQLContainer1 = _

  val origGetConfig: () => Config = Utils.getConfig
  val origGetNatsPort: () => Int = Utils.getNatsPort
  val origGetBootstrapServers: () => String = Utils.getBootstrapServers

  implicit val executionContext: ExecutionContextExecutor =
    ExecutionContext.fromExecutor(Executors.newFixedThreadPool(4))

  def setUpMockGetConfig(port: Int): Unit = {
    val mockConfig = mock[Config]
    val mockGetConfig = mock[() => Config]
    mockGetConfig.apply() returns mockConfig
    mockConfig.getString(DB_DRIVER) returns "org.postgresql.Driver"
    mockConfig.getString(DB_URL) returns s"jdbc:postgresql://localhost:$port/twitter1"
    mockConfig.getString(DB_USERNAME) returns "postgres1"
    mockConfig.getString(DB_PASSWORD) returns "root1"
    Utils.getConfig = mockGetConfig
  }

  def setUpMockGetNatsPort(port: Int): Unit = {
    val mockGetNatsPort = mock[() => Int]
    mockGetNatsPort.apply() returns port
    Utils.getNatsPort = mockGetNatsPort
  }

  def setUpMockGetBootstrapServers(bootstrapServers: String): Unit = {
    val mockGetBootstrapServers = mock[() => String]
    mockGetBootstrapServers.apply() returns bootstrapServers
    Utils.getBootstrapServers = mockGetBootstrapServers
  }

  before {
    natsContainer = new GenericContainer1(NATS_IMAGE)
      .withExposedPorts(NATS_PORT)
      .withLogConsumer(new Slf4jLogConsumer(logger))
      .waitingFor(new LogMessageWaitStrategy().withRegEx(".*Server is ready.*"));

    kafkaContainer = new KafkaContainer(KAFKA_IMAGE)
      .withEmbeddedZookeeper()
      .withLogConsumer(new Slf4jLogConsumer(logger))

    postgresContainer = new PostgreSQLContainer1(POSTGRES_IMAGE)
      .withDatabaseName(TEST_DB_NAME)
      .withUsername(TEST_DB_USERNAME)
      .withPassword(TEST_DB_PASSWORD)
      .withExposedPorts(POSTGRES_PORT)
      .withLogConsumer(new Slf4jLogConsumer(logger))

    natsContainer.start()
    kafkaContainer.start()
    postgresContainer.start()

    setUpMockGetNatsPort(natsContainer.getMappedPort(NATS_PORT))
    setUpMockGetConfig(postgresContainer.getMappedPort(POSTGRES_PORT))
  }

  after {
    postgresContainer.stop()
    kafkaContainer.stop()
    natsContainer.stop()

    Utils.getConfig = origGetConfig
    Utils.getNatsPort = origGetNatsPort
    Utils.getBootstrapServers = origGetBootstrapServers
  }

  /*
  "NATS, Kafka and Postgres container" should "be running" in {
    assert(natsContainer.isRunning)
    assert(kafkaContainer.isRunning)
    assert(postgresContainer.isRunning)
  }
   */

  "Postgres" should s"have ${TWEETS_COUNT} elements after NatsKafkaDemo runs" in {
    NatsKafkaDemo.main(Array())

    val db = Utils.getDB
    val tweetsTable = TableQuery[TweetsTable]

    db.run(tweetsTable.result) onComplete {
      case scala.util.Success(tweetSeq) =>
        assert(tweetSeq.size == TWEETS_COUNT)
      case scala.util.Failure(exception) =>
        logger.error("Error querying database", exception)
        throw exception
    }
  }

  class GenericContainer1(dockerImageName: DockerImageName)
    extends GenericContainer[GenericContainer1](dockerImageName)

  class PostgreSQLContainer1(dockerImageName: DockerImageName)
    extends PostgreSQLContainer[PostgreSQLContainer1](dockerImageName)
}