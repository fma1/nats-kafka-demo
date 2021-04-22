import com.danielasfregola.twitter4s.entities.Tweet
import com.danielasfregola.twitter4s.processors.TwitterProcessor.{json4sFormats, serialization => theSerialization}
import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode, ObjectMapper, PropertyNamingStrategy}
import io.github.azhur.kafkaserdejson4s.Json4sSupport
import io.nats.client.{Connection, Dispatcher, MessageHandler, Nats}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.Serdes.StringSerde
import org.apache.kafka.common.serialization.{Serde, Serdes, StringDeserializer, StringSerializer}
import org.apache.kafka.connect.json.{JsonDeserializer, JsonSerializer}
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig}
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Consumed._
import org.json4s.{Formats, Serialization}
import org.slf4j.{Logger, LoggerFactory}

import java.util.Properties
import scala.compat.java8.DurationConverters.FiniteDurationops
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.io.{BufferedSource, Source}
import scala.jdk.CollectionConverters._

object NatsKafkaDemo {
  val logger: Logger = LoggerFactory.getLogger("NatsKafkaDemo")

  val BOOTSTRAP_SERVER = "127.0.0.1:9092"
  val FROM_TOPIC = "tweets"
  val TO_TOPIC = "retweets"
  val GROUP_ID = "nats-kafka-demo-1"

  val producerProps = new Properties()
  producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER)
  producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

  val consumerProps = new Properties()
  consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER)
  consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID)
  consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  val streamsProps: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-filter-tweets")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER)
    p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, classOf[StringSerde])
    p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, classOf[StringSerde])
    p
  }

  implicit val serialization: Serialization = theSerialization
  // val serde: Serde[Tweet] = Json4sSupport.toSerde
  // val serde: Serde[String] = new StringSerde()

  val jsonObjectMapper = new ObjectMapper
  val jsonSerde: Serde[JsonNode] = Serdes.serdeFrom(new JsonSerializer(), new JsonDeserializer())
  jsonObjectMapper.setPropertyNamingStrategy(PropertyNamingStrategy.UPPER_CAMEL_CASE)
  jsonObjectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  def main(args: Array[String]): Unit = {
    // Kafka Stuff
    val builder = new StreamsBuilder

    builder.stream(FROM_TOPIC, Consumed.`with`(jsonSerde, jsonSerde))
      .filter((_, jsonNode) => {
        System.err.println(jsonNode)
        jsonNode.get("retweeted").asBoolean()
      })
      .to(TO_TOPIC)

    val producer = new KafkaProducer[String, String](producerProps)
    val consumer = new KafkaConsumer[String, String](consumerProps)
    val streams = new KafkaStreams(builder.build(), streamsProps)
    consumer.subscribe(List(TO_TOPIC).asJava)
    streams.start()

    Future {
      while (true) consumer.poll((100 milliseconds).toJava).asScala.foreach(record => {
        logger.info(s"Key: ${record.key()}, Value: ${record.value()}")
        logger.info(s"Partition: ${record.partition()}, Offset: ${record.offset()}")
      })
    }

    // NATS stuff
    val nc: Connection = Nats.connect("nats://localhost:4222")
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

    /*
    val source: BufferedSource = Source.fromURL(getClass.getResource("tweets.json"))
    try {
      source
        .getLines()
        .foreach(tweet => nc.publish(FROM_TOPIC, tweet.getBytes))
    } finally {
      source.close()
    }

     */

    Thread.sleep(20000)
  }
}
