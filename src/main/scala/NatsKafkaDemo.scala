import io.nats.client.{Connection, Dispatcher, MessageHandler, Nats}
import org.slf4j.{Logger, LoggerFactory}

import scala.io.{BufferedSource, Source}

object NatsKafkaDemo {
  val logger: Logger = LoggerFactory.getLogger("NatsKafkaDemo")

  def main(args: Array[String]): Unit = {
    val nc: Connection = Nats.connect("nats://localhost:4222")
    scala.sys.addShutdownHook({
      nc.close()
    })

    val dispatcher: Dispatcher = {
      nc.createDispatcher(msg => logger.info(s"Message: ${new String(msg.getData)}"))
    }
    // TODO: Add constant for this
    dispatcher.subscribe("tweets")

    val source: BufferedSource = Source.fromURL(getClass.getResource("tweets.json"))
    try {
      source
        .getLines()
        .foreach(tweet => nc.publish("tweets", tweet.getBytes))
    } finally {
      source.close()
    }

    Thread.sleep(20000)
  }
}
