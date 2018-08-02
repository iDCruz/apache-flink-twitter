package com.davidcruz.apacheflinktwitter.model

import org.apache.flink.api.common.functions.MapFunction

class MapTweetToInfluxDbPoint implements MapFunction<Tweet, InfluxDbPoint>{
    @Override
    InfluxDbPoint map(Tweet value) throws Exception {
        Map<String, String> tags = new HashMap<>()
        Map<String, Object> fields = new HashMap<>()

        tags.put("source", value.source)
        fields.put("value", value.num)

        return new InfluxDbPoint("count", System.currentTimeMillis(), tags, fields)
    }
}
