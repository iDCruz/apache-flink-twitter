package com.davidcruz.apacheflinktwitter.utility

import org.apache.flink.streaming.connectors.twitter.TwitterSource
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
@ConfigurationProperties(prefix = "twitter")
class TwitterConfig {
    String consumerKey
    String consumerSecret
    String token
    String tokenSecret

    @Bean
    TwitterSource twitterSource(){
        Properties props = new Properties()
        props.setProperty(TwitterSource.CONSUMER_KEY, consumerKey);
        props.setProperty(TwitterSource.CONSUMER_SECRET, consumerSecret);
        props.setProperty(TwitterSource.TOKEN, token);
        props.setProperty(TwitterSource.TOKEN_SECRET, tokenSecret);

        return new TwitterSource(props)
    }

}
