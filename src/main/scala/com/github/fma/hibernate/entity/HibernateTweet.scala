package com.github.fma.hibernate.entity

import java.sql.Timestamp
import javax.persistence.{Column, Entity, GeneratedValue, GenerationType, Id, Table}
import scala.beans.BeanProperty

@Entity
@Table(name = "HibernateTweet")
class HibernateTweet(
              _created_at: Timestamp,
              _id_str: String,
              _in_reply_to_status_id: java.lang.Long,
              _in_reply_to_status_id_str: String,
              _in_reply_to_user_id: java.lang.Long,
              _in_reply_to_user_id_str: String,
              _in_reply_to_screen_name: String,
              _is_quote_status: java.lang.Boolean,
              _quoted_status_id: java.lang.Long,
              _quoted_status_id_str: String,
              _the_user_id: java.lang.Long,
              _favorite_count: java.lang.Long,
              _retweet_count: java.lang.Long,
              _retweeted: java.lang.Boolean,
              _source: String,
              _text: String,
            ) {
    @Id
    @BeanProperty
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    var id: java.lang.Long = _

    @BeanProperty
    @Column(name = "created_at") var created_at: Timestamp = _created_at
    @BeanProperty
    @Column(name = "id_str") var id_str: String = _id_str
    @BeanProperty
    @Column(name = "in_reply_to_status_id") var in_reply_to_status_id: java.lang.Long = _in_reply_to_status_id
    @BeanProperty
    @Column(name = "in_reply_to_status_id_str") var in_reply_to_status_id_str: String = _in_reply_to_status_id_str
    @BeanProperty
    @Column(name = "in_reply_to_user_id") var in_reply_to_user_id: java.lang.Long = _in_reply_to_user_id
    @BeanProperty
    @Column(name = "in_reply_to_user_id_str") var in_reply_to_user_id_str: String = _in_reply_to_user_id_str
    @BeanProperty
    @Column(name = "in_reply_to_screen_name") var in_reply_to_screen_name: String = _in_reply_to_screen_name
    @BeanProperty
    @Column(name = "is_quote_status") var is_quote_status: java.lang.Boolean = _is_quote_status
    @BeanProperty
    @Column(name = "quoted_status_id") var quoted_status_id: java.lang.Long = _quoted_status_id
    @BeanProperty
    @Column(name = "quoted_status_id_str") var quoted_status_id_str: String = _quoted_status_id_str
    @BeanProperty
    @Column(name = "the_user_id") var the_user_id: java.lang.Long = _the_user_id
    @BeanProperty
    @Column(name = "favorite_count") var favorite_count: java.lang.Long = _favorite_count
    @BeanProperty
    @Column(name = "retweet_count") var retweet_count: java.lang.Long = _retweet_count
    @BeanProperty
    @Column(name = "retweeted") var retweeted: java.lang.Boolean = _retweeted
    @BeanProperty
    @Column(name = "source") var source: String = _source
    @BeanProperty
    @Column(name = "text") var text: String = _text

    private def this() = this(
        null,
        "",
        0L,
        "",
        0L,
        "",
        "",
        false,
        0L,
        "",
        0L,
        0L,
        0L,
        false,
        "",
        ""
    )
}