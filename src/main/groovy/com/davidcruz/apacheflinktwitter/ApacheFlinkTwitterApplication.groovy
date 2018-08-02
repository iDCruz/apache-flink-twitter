package com.davidcruz.apacheflinktwitter

import com.davidcruz.apacheflinktwitter.model.MapToTweet
import com.davidcruz.apacheflinktwitter.model.MapTweetToInfluxDbPoint
import com.davidcruz.apacheflinktwitter.model.TweetFilter
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import groovy.util.logging.Slf4j
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.twitter.TwitterSource
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication

import javax.annotation.PostConstruct
import java.util.concurrent.TimeUnit

@Slf4j
@SpringBootApplication
class ApacheFlinkTwitterApplication {

    static void main(String[] args) {
        SpringApplication.run ApacheFlinkTwitterApplication, args
    }

    @Autowired
    TwitterSource twitterSource

    @PostConstruct
    void init() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
        DataStream<String> stream = env.addSource(twitterSource)
//        stream.filter(new TweetFilter())
//                .print()

        stream.filter(new TweetFilter())
            .map(new MapToTweet())
            .keyBy("source")
            .timeWindow(Time.seconds(10))
            .sum("num")
            .map(new MapTweetToInfluxDbPoint())
            .addSink(new InfluxDbSink())

        env.execute()
    }
}
