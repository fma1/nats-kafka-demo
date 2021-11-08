package com.github.fma.hibernate

import com.github.fma.hibernate.NatsKafkaHibernateDemo.{sessionFactory, toHibernateTweet}
import com.github.fma.hibernate.entity.HibernateTweet
import com.github.fma.slickpkg.Utils._
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

import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.jdk.CollectionConverters._

class NatsKafkaHibernateDemoSpec extends AnyFlatSpec with should.Matchers with BeforeAndAfter with Mockito {
  val logger: Logger = LoggerFactory.getLogger(classOf[NatsKafkaHibernateDemoSpec])
  val logConsumer = new Slf4jLogConsumer(logger)

  /*
   * NOTE: I changed the test database name, username and password
   * to show the example of where the test docker database would be different
   * from the actual database
   */
  val TEST_DB_NAME = "twitter1"
  val TEST_DB_USERNAME = "postgres1"
  val TEST_DB_PASSWORD = "root1"

  // There are 196 lines in tweets.json
  // But running "grep "RT @" tweets.json | wc" outputs 103
  val RETWEETS_COUNT = 103

  val NATS_IMAGE: DockerImageName = DockerImageName.parse("nats:2.6.2")
  val KAFKA_IMAGE: DockerImageName = DockerImageName.parse("confluentinc/cp-kafka:6.2.1")
  val POSTGRES_IMAGE: DockerImageName = DockerImageName.parse("postgres:9.6.21")

  var natsContainer: GenericContainer1 = _
  var kafkaContainer: KafkaContainer = _
  var postgresContainer: PostgreSQLContainer1 = _

  val origGetConfig: () => Config = getConfig
  val origGetNatsPort: () => Int = getNatsPort
  val origGetBootstrapServers: () => String = getBootstrapServers

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
    getConfig = mockGetConfig
  }

  def setUpMockGetNatsPort(port: Int): Unit = {
    val mockGetNatsPort = mock[() => Int]
    mockGetNatsPort.apply() returns port
    getNatsPort = mockGetNatsPort
  }

  def setUpMockGetBootstrapServers(bootstrapServers: String): Unit = {
    val mockGetBootstrapServers = mock[() => String]
    mockGetBootstrapServers.apply() returns bootstrapServers
    getBootstrapServers = mockGetBootstrapServers
  }

  before {
    natsContainer = new GenericContainer1(NATS_IMAGE)
      .withExposedPorts(NATS_PORT)
      .withLogConsumer(logConsumer)
      .waitingFor(new LogMessageWaitStrategy().withRegEx(".*Server is ready.*"));

    kafkaContainer = new KafkaContainer(KAFKA_IMAGE)
      .withEmbeddedZookeeper()
      .withLogConsumer(logConsumer)

    postgresContainer = new PostgreSQLContainer1(POSTGRES_IMAGE)
      .withDatabaseName(TEST_DB_NAME)
      .withUsername(TEST_DB_USERNAME)
      .withPassword(TEST_DB_PASSWORD)
      .withExposedPorts(POSTGRES_PORT)
      .withLogConsumer(logConsumer)

    natsContainer.start()
    kafkaContainer.start()
    postgresContainer.start()

    System.err.println(s"NATS_PORT: ${natsContainer.getMappedPort(NATS_PORT)}")

    setUpMockGetBootstrapServers(kafkaContainer.getBootstrapServers)
    setUpMockGetNatsPort(natsContainer.getMappedPort(NATS_PORT))
    setUpMockGetConfig(postgresContainer.getMappedPort(POSTGRES_PORT))
  }

  after {
    postgresContainer.stop()
    kafkaContainer.stop()
    natsContainer.stop()

    getConfig = origGetConfig
    getNatsPort = origGetNatsPort
    getBootstrapServers = origGetBootstrapServers
  }

  "NATS, Kafka and Postgres container" should "be running" in {
    assert(natsContainer.isRunning)
    assert(kafkaContainer.isRunning)
    assert(postgresContainer.isRunning)
  }

  "Postgres" should s"have ${RETWEETS_COUNT} elements after NatsKafkaSlickDemo runs" in {
    NatsKafkaHibernateDemo.main(Array())

    var lst: scala.collection.mutable.Buffer[HibernateTweet] = null

    val session = NatsKafkaHibernateDemo.sessionFactory.getCurrentSession
    try {
      session.beginTransaction()
      lst = session.createQuery("from HibernateTweet", classOf[HibernateTweet]).getResultList.asScala
    } finally {
      session.close()
    }

    println(NatsKafkaHibernateDemo.natsCounter.get())
    println(NatsKafkaHibernateDemo.kafkaConsumerCounter.get())

    assert(lst.size == RETWEETS_COUNT)
  }

  class GenericContainer1(dockerImageName: DockerImageName)
    extends GenericContainer[GenericContainer1](dockerImageName)

  class PostgreSQLContainer1(dockerImageName: DockerImageName)
    extends PostgreSQLContainer[PostgreSQLContainer1](dockerImageName)
}
