package com.github.fma

import com.github.fma.Utils.TweetsTable
import com.typesafe.config.{Config, ConfigFactory}
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
import scala.reflect.runtime.universe._

class NatsKafkaDemoSpec extends AnyFlatSpec with should.Matchers with BeforeAndAfter with Mockito {
  val logger: Logger = LoggerFactory.getLogger(classOf[NatsKafkaDemoSpec])

  val TWEETS_COUNT = 196
  val NATS_IMAGE: DockerImageName = DockerImageName.parse("nats:2.2.2")
  val KAFKA_IMAGE: DockerImageName = DockerImageName.parse("confluentinc/cp-kafka:5.4.3")
  val POSTGRES_IMAGE: DockerImageName = DockerImageName.parse("postgres:9.6.21")

  var natsContainer: GenericContainer1 = _
  var kafkaContainer: KafkaContainer = _
  var postgresContainer: PostgreSQLContainer1 = _

  val origGetConfig: () => Config = Utils.getConfig

  implicit val executionContext: ExecutionContextExecutor =
    ExecutionContext.fromExecutor(Executors.newFixedThreadPool(4))

  before {
    // TODO: Add NATS_SERVER so it can be provided by test
    natsContainer = new GenericContainer1(NATS_IMAGE)
      .withExposedPorts(4222)
      .withLogConsumer(new Slf4jLogConsumer(logger))
      .waitingFor(new LogMessageWaitStrategy().withRegEx(".*Server is ready.*"));

    // TODO: Change BOOTSTRAP_SERVER so it can be provided by test
    kafkaContainer = new KafkaContainer(KAFKA_IMAGE)
      .withEmbeddedZookeeper()
      .withLogConsumer(new Slf4jLogConsumer(logger))

    /*
     * NOTE: I changed the test database name, username and password
     * to show the example of where the test docker database would be different
     * from the actual database
     */
    postgresContainer = new PostgreSQLContainer1(POSTGRES_IMAGE)
      .withDatabaseName("twitter1")
      .withUsername("postgres1")
      .withPassword("root1")
      .withExposedPorts(5432)
      .withLogConsumer(new Slf4jLogConsumer(logger))

    natsContainer.start()
    kafkaContainer.start()
    postgresContainer.start()

    val testConfig = ConfigFactory.load("application-test")
    val mockGetConfig = mock[() => Config]
    mockGetConfig.apply() returns testConfig

    Utils.getConfig = mockGetConfig
  }

  after {
    postgresContainer.stop()
    kafkaContainer.stop()
    natsContainer.stop()

    Utils.getConfig = origGetConfig
  }

  "NATS, Kafka and Postgres container" should "be running" in {
    assert(natsContainer.isRunning)
    assert(kafkaContainer.isRunning)
    assert(postgresContainer.isRunning)
  }

  /*
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
   */

  // TOOD: Modify NATS and Postgres to provide port number
  /*
  def addMyFixedExposedPort(genericContainer: GenericContainer[_], hostPort: Int, containerPort: Int): Unit = {
    genericContainer.getClass.getMethods.foreach(println)
    val addFixedExposedPortMethod =
      genericContainer.getClass.getMethod("addFixedExposedPort", Integer.TYPE, Integer.TYPE)
    addFixedExposedPortMethod.setAccessible(true)
    addFixedExposedPortMethod.invoke(genericContainer, hostPort, containerPort)
    ()
  }

   */

  class GenericContainer1(dockerImageName: DockerImageName)
    extends GenericContainer[GenericContainer1](dockerImageName)

  class PostgreSQLContainer1(dockerImageName: DockerImageName)
    extends PostgreSQLContainer[PostgreSQLContainer1](dockerImageName)
}