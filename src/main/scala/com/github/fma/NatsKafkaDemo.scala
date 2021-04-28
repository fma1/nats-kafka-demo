package com.github.fma

import com.github.fma.Utils._
import com.danielasfregola.twitter4s.entities.Tweet
import com.danielasfregola.twitter4s.processors.TwitterProcessor.{json4sFormats, serialization => theSerialization}
import io.github.azhur.kafkaserdejson4s.Json4sSupport
import io.nats.client.{Connection, Nats}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.Serdes.StringSerde
import org.apache.kafka.common.serialization.{Serde, StringDeserializer, StringSerializer}
import org.apache.kafka.streams.kstream.Consumed._
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig}
import org.json4s.Serialization
import org.slf4j.{Logger, LoggerFactory}
import slick.jdbc.PostgresProfile.api._

import java.time.Duration
import java.util.Properties
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.io.{BufferedSource, Source}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success}

object NatsKafkaDemo {
  private[this] val logger: Logger = LoggerFactory.getLogger("NatsKafkaDemo")

  val db = getDB
  val tweetsTable = TableQuery[TweetsTable]

  implicit val serialization: Serialization = theSerialization
  val serde: Serde[Tweet] = Json4sSupport.toSerde

  def main(args: Array[String]): Unit = {
    // Kafka Stuff
    /*
     * I was thinking of abstracting generating these props into a method,
     * but for the purposes of this demo, such a method isn't necessary.
     */
    val producerProps = new Properties()
    producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers())
    producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    val consumerProps = new Properties()
    consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers())
    consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID)
    consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val streamsProps: Properties = {
      val p = new Properties()
      p.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID)
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers())
      p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, classOf[StringSerde])
      p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, classOf[StringSerde])
      p
    }

    val builder = new StreamsBuilder
    builder.stream(FROM_TOPIC, `with`(serde, serde))
      .filter((_, tweet) => {
        logger.info(s"KafkaStreams Tweet: $tweet")
        tweet.text.contains("RT @")
      })
      .to(TO_TOPIC)

    val producer = new KafkaProducer[String, String](producerProps)
    val consumer = new KafkaConsumer[String, String](consumerProps)
    val streams = new KafkaStreams(builder.build(), streamsProps)
    consumer.subscribe(List(TO_TOPIC).asJavaCollection)
    streams.start()

    // NATS stuff
    // TODO: Might want to abstract later to provide any URL and not just port
    val nc: Connection = Nats.connect(s"nats://localhost:${getNatsPort()}")
    scala.sys.addShutdownHook({
      nc.close()
      streams.close()
    })

    val dispatcher =
      nc.createDispatcher(msg => {
        val msgStr = new String(msg.getData)
        logger.info(s"Received Message on NATS topic ${msg.getSubject}: ${new String(msg.getData)}")
        producer.send(new ProducerRecord[String, String](FROM_TOPIC, msgStr))
        logger.info("Sent Message to Kafka Producer")
      })
    dispatcher.subscribe(FROM_TOPIC)

    // ------------------------------------------------------------------------

    Future {
      try {
        while (true) {
          val records = consumer.poll(Duration.ofMillis(TIMEOUT_MILLS))
          records.iterator().forEachRemaining { record: ConsumerRecord[String, String] =>
            logger.info(
              s"""
                 |message
                 | offset=${record.offset}
                 | partition=${record.partition}
                 | key=${record.key}
                 | value=${record.value}
              """.stripMargin)

            val tweet: Tweet = serialization.read[Tweet](record.value)

            logger.info(
              s"""Adding Tweet with offset ${record.offset} and partition ${record.partition} and key ${record.key}...""".stripMargin)

            db.run(tweetsTable ++= List(tweet.toTweetTuple)) onComplete {
              case Success(_) =>
                logger.info(
                  s"""Successfully added Tweet with offset ${record.offset} and partition ${record.partition} and key ${record.key}""".stripMargin)
              case Failure(exception) =>
                logger.error(
                  s"""Error adding Tweet with offset ${record.offset}|and partition ${record.partition} and key ${record.key}""".stripMargin, exception)
            }
          }
        }
      } finally db.close
    } onComplete {
      case Success(_) =>
        logger.info("Successfully processed all tweets from Kafka Consumer")
      case Failure(exception) =>
        logger.error("Error processing all tweets from Kafka Consumer", exception)
    }

    // TODO: Ideally tweets.json in tests should be a symlink, but I don't think that'd work with both Unix/Windows
    val tweetInputStream =
      getClass.getClassLoader.getResourceAsStream("tweets.json")
    val source: BufferedSource = Source.fromInputStream(tweetInputStream)
    try {
      source
        .getLines()
        .foreach(tweet => nc.publish(FROM_TOPIC, tweet.getBytes))
    } finally {
      source.close()
    }

    Thread.sleep(2000)
  }
}
