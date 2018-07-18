package com.davidcruz.apacheflinktwitter

import groovy.util.logging.Slf4j
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.java.ExecutionEnvironment
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication

import javax.annotation.PostConstruct

@Slf4j
@SpringBootApplication
class ApacheFlinkTwitterApplication {

    static void main(String[] args) {
        SpringApplication.run ApacheFlinkTwitterApplication, args
    }

    @PostConstruct
    static void start() {
        ExecutionEnvironment env = FlinkExecutionEnvironment()
        DataSet<Integer> ds = env.fromElements(1, 2, 3, 4, 5, 6)

        log.info("Pre-Transform")
        log.info(ds.collect().join(","))

        log.info("Post-Transform: Filter")
        log.info(SampleFilter(ds).join(","))

        log.info("Post-Transform: Map")
        log.info(SampleMap(ds).join(","))

    }

    static List SampleMap(DataSet<Integer> ds){
        List<Integer> list = ds.map(new MapFunction<Integer, Object>() {
            @Override
            Object map(Integer value) throws Exception {
                return value.doubleValue()
            }
        }).collect()

        list
    }

    static List SampleFilter(DataSet<Integer> ds){
        List<Integer> list = ds.filter(new FilterFunction<Integer>() {
            @Override
            boolean filter(Integer value) throws Exception {
                return value.intValue() == 2
            }
        }).collect()

        list
    }

    static ExecutionEnvironment FlinkExecutionEnvironment() {
        ExecutionEnvironment.getExecutionEnvironment()
    }
}
