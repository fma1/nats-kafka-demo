CREATE DATABASE twitter;

CREATE TABLE tweets (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    id_str VARCHAR,
    in_reply_to_status_id BIGINT,
    in_reply_to_status_id_str VARCHAR,
    in_reply_to_user_id BIGINT,
    in_reply_to_user_id_str VARCHAR,
    in_reply_to_screen_name VARCHAR,
    is_quote_status BOOLEAN,
    quoted_status_id BIGINT,
    quoted_status_id_str VARCHAR,
    the_user_id BIGINT,
    favorite_count BIGINT,
    retweet_count BIGINT,
    retweeted BOOLEAN,
    source VARCHAR,
    text VARCHAR
);