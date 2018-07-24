package com.davidcruz.apacheflinktwitter.tutorials.tweets

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Configuration

@Configuration
@ConfigurationProperties(prefix = "twitter")
class TwitterConfig {
    String key
    String secret

}
