package com.davidcruz.apacheflinktwitter.model

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.api.common.functions.MapFunction

class MapToTweet implements MapFunction<String, Tweet> {
    static private final ObjectMapper objectMapper = new ObjectMapper()

    @Override
    Tweet map(String value) throws Exception {
        JsonNode jsonNode = objectMapper.readTree(value)
        Tweet tweet = new Tweet(jsonNode.get("source").textValue(), 1)

        tweet
    }
}
