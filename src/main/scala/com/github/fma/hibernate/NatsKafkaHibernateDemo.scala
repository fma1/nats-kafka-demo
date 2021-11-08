package com.github.fma.hibernate

import com.github.fma.slickpkg.Utils._
import com.danielasfregola.twitter4s.entities.Tweet
import com.danielasfregola.twitter4s.processors.TwitterProcessor.{json4sFormats, serialization => theSerialization}
import com.github.fma.hibernate.entity.HibernateTweet
import com.github.fma.hibernate.NatsKafkaHibernateDemo._
import io.github.azhur.kafkaserdejson4s.Json4sSupport
import io.nats.client.{Connection, Nats}
import org.apache.kafka.clients.admin.{Admin, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.serialization.Serdes.StringSerde
import org.apache.kafka.common.serialization.{Serde, StringDeserializer, StringSerializer}
import org.apache.kafka.streams.kstream.Consumed._
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig}
import org.hibernate.SessionFactory
import org.hibernate.cfg.Configuration
import org.json4s.Serialization
import org.slf4j.{Logger, LoggerFactory}

import java.sql.Timestamp
import java.time.Duration
import java.util
import java.util.{Collections, Properties}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.io.{BufferedSource, Source}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success}

object NatsKafkaHibernateDemo {
    private[this] val logger: Logger = LoggerFactory.getLogger("NatsKafkaSlickDemo")

    val sessionFactory: SessionFactory = new Configuration()
      .setProperty("hibernate.connection.url", getConfig().getString(DB_URL))
      .setProperty("hibernate.connection.driver_class", getConfig().getString(DB_DRIVER))
      .setProperty("hibernate.connection.username", getConfig().getString(DB_USERNAME))
      .setProperty("hibernate.connection.password", getConfig().getString(DB_PASSWORD))
      .setProperty("hibernate.dialect", "org.hibernate.dialect.PostgreSQL95Dialect")
      .setProperty("hibernate.pool_size", "1")
      .setProperty("hibernate.show_sql", "true")
      .setProperty("hibernate.current_session_context_class", "thread")
      // So Hibernate will automatically create the table
      .setProperty("hibernate.hbm2ddl.auto", "create")
      .addAnnotatedClass(classOf[HibernateTweet])
      .buildSessionFactory()

    var readCounter = new AtomicInteger()
    var natsCounter = new AtomicInteger()
    var kafkaStreamsCounter = new AtomicInteger()
    var kafkaConsumerCounter = new AtomicInteger()

    implicit val serialization: Serialization = theSerialization
    val serde: Serde[Tweet] = Json4sSupport.toSerde

    def toHibernateTweet(tweet: Tweet): HibernateTweet = {
        new HibernateTweet(
            Timestamp.from(tweet.created_at),
            tweet.id_str,
            tweet.in_reply_to_status_id.getOrElse(0L).asInstanceOf[Long],
            tweet.in_reply_to_status_id_str.getOrElse(""),
            tweet.in_reply_to_user_id.getOrElse(0L).asInstanceOf[Long],
            tweet.in_reply_to_user_id_str.getOrElse(""),
            tweet.in_reply_to_screen_name.getOrElse(""),
            tweet.is_quote_status,
            tweet.quoted_status_id.getOrElse(0L).asInstanceOf[Long],
            tweet.quoted_status_id_str.getOrElse(""),
            tweet.user.map(_.id).getOrElse(0L).asInstanceOf[Long],
            tweet.favorite_count,
            tweet.retweet_count,
            tweet.retweeted,
            tweet.source,
            tweet.text
        )
    }

    def main(args: Array[String]): Unit = {
        // Kafka Stuff
        /*
     * I was thinking of abstracting generating these props into a method,
     * but for the purposes of this demo, such a method isn't necessary.
     */

        val numPart: Int = 1
        val repliFactor: Short = 1
        val newTopics = Vector(
            new NewTopic(FROM_TOPIC, numPart, repliFactor),
            new NewTopic(TO_TOPIC, numPart, repliFactor)
        )
        val props = new Properties()
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers())
        val client = Admin.create(props)

        client.createTopics(newTopics.asJava).values().asScala.foreach {
            case (_, kafkaFuture) =>
                kafkaFuture.whenComplete {
                    case (_, throwable: Throwable) if Option(throwable).isDefined => // failure
                    case _ => // it's ok
                }
        }

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
              System.err.println(s"Kafka Streams Counter: ${kafkaStreamsCounter.getAndIncrement()}")
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
                System.err.println(s"NATS Counter: ${natsCounter.getAndIncrement()}")
                logger.info(s"Received Message on NATS topic ${msg.getSubject}: $msgStr")
                producer.send(new ProducerRecord[String, String](FROM_TOPIC, msgStr))
                logger.info("Sent Message to Kafka Producer")
            })
        dispatcher.subscribe(FROM_TOPIC)

        // ------------------------------------------------------------------------

        Future {
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
                        System.err.println(s"Kafka Consumer Counter: ${kafkaConsumerCounter.getAndIncrement()}")

                        val tweet: Tweet = serialization.read[Tweet](record.value)

                        val session = sessionFactory.getCurrentSession
                        try {
                            session.beginTransaction()
                            session.save(toHibernateTweet(tweet))
                            session.getTransaction.commit()
                        } finally {
                            session.close()
                        }

                        logger.info(
                            s"""Adding Tweet with offset ${record.offset} and partition ${record.partition} and key ${record.key}...""".stripMargin)

                    }
                }
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
              .foreach(tweet => {
                  System.err.println(s"Read Counter: ${readCounter.getAndIncrement()}")
                  nc.publish(FROM_TOPIC, tweet.getBytes)
              })
        } finally {
            source.close()
        }

        Thread.sleep(10000)
    }
}
