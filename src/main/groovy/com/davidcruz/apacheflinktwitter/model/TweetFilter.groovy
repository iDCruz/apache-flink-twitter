package com.davidcruz.apacheflinktwitter.model

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.api.common.functions.FilterFunction

class TweetFilter implements FilterFunction<String> {
    static private final ObjectMapper objectMapper = new ObjectMapper()
    @Override
    boolean filter(String value) throws Exception {
        JsonNode jsonNode = objectMapper.readTree(value)

        jsonNode.get("lang")?.textValue()  == "en"
    }
}
