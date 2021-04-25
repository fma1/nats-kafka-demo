package com.github.fma

import com.danielasfregola.twitter4s.entities.Tweet
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.{Logger, LoggerFactory}
import slick.jdbc.PostgresProfile.api._

import java.sql.Timestamp
import scala.language.implicitConversions

class Utils {}

object Utils {
  type TweetTuple = (Long, Timestamp, String, Option[Long], Option[String], Option[Long], Option[String], Option[String], Boolean, Option[Long], Option[String], Option[Long], Int, Long, Boolean, String, String)

  val logger: Logger = LoggerFactory.getLogger(classOf[Utils])

  // var for mocking purposes
  private[fma] var getConfig: () => Config = () => ConfigFactory.load("application")

  def getDB = {
    val config = getConfig()
    val url = config.getString("db.url")
    val username = config.getString("db.username")
    val password = config.getString("db.password")
    val driver = config.getString("db.driver")

    Database.forURL(url, username, password, null, driver)
  }

  implicit class TweetImprovement(val tweet: Tweet) {
    def toTweetTuple: TweetTuple = {
      tweet match {
        case Tweet(
        _, _,
        created_at,
        _, _, _, _,
        favorite_count,
        _, _, _,
        id,
        id_str,
        in_reply_to_screen_name,
        in_reply_to_status_id,
        in_reply_to_status_id_str,
        in_reply_to_user_id,
        in_reply_to_user_id_str,
        is_quote_status,
        _, _, _,
        quoted_status_id,
        quoted_status_id_str,
        _, _,
        retweet_count,
        retweeted,
        _,
        source,
        text,
        _, _,
        user,
        _, _, _, _
        ) =>
          (
            id,
            Timestamp.from(created_at),
            id_str,
            in_reply_to_status_id,
            in_reply_to_status_id_str,
            in_reply_to_user_id,
            in_reply_to_user_id_str,
            in_reply_to_screen_name,
            is_quote_status,
            quoted_status_id,
            quoted_status_id_str,
            user.map(_.id),
            favorite_count,
            retweet_count,
            retweeted,
            source,
            text
          )
      }
    }
  }

  class TweetsTable(tag: Tag) extends Table[TweetTuple](tag, "tweets") {
    def id = column[Long]("id", O.PrimaryKey) // This is the primary key column
    def created_at = column[Timestamp]("created_at")
    def id_str = column[String]("id_str")
    def in_reply_to_status_id = column[Long]("in_reply_to_status_id")
    def in_reply_to_status_id_str = column[String]("in_reply_to_status_id_str")
    def in_reply_to_user_id = column[Long]("in_reply_to_user_id")
    def in_reply_to_user_id_str = column[String]("in_reply_to_user_id_str")
    def in_reply_to_screen_name = column[String]("in_reply_to_screen_name")
    def is_quote_status = column[Boolean]("is_quote_status")
    def quoted_status_id = column[Long]("quoted_status_id")
    def quoted_status_id_str = column[String]("quoted_status_id_str")
    def the_user_id = column[Long]("the_user_id")
    def favorite_count = column[Int]("favorite_count")
    def retweet_count = column[Long]("retweet_count")
    def retweeted = column[Boolean]("retweeted")
    def source = column[String]("source")
    def text = column[String]("text")

    def * = (id, created_at, id_str, in_reply_to_status_id.?, in_reply_to_status_id_str.?, in_reply_to_user_id.?, in_reply_to_user_id_str.?, in_reply_to_screen_name.?, is_quote_status, quoted_status_id.?, quoted_status_id_str.?, the_user_id.?, favorite_count, retweet_count, retweeted, source, text)
  }
}
