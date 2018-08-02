package com.davidcruz.apacheflinktwitter

import com.davidcruz.apacheflinktwitter.model.InfluxDbPoint
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.influxdb.InfluxDB
import org.influxdb.InfluxDBFactory
import org.influxdb.dto.Point

import java.util.concurrent.TimeUnit

class InfluxDbSink extends RichSinkFunction<InfluxDbPoint>{

    InfluxDB influxDB

    @Override
    void open(Configuration parameters) throws Exception {
        super.open(parameters)
        influxDB = InfluxDBFactory.connect("http://localhost:8086/")
        String dbName = "twitter"
        influxDB.setDatabase(dbName)
    }

    @Override
    void close() throws Exception {
        influxDB.close()
    }

    @Override
    void invoke(InfluxDbPoint value, Context context) throws Exception {
        Point.Builder builder = Point.measurement(value.measurement)
            .time(value.timestamp, TimeUnit.MILLISECONDS)

        if(value.fields){
            builder.fields(value.fields)
        }

        if(value.tags){
            builder.tag(value.tags)
        }

        influxDB.write(builder.build())
    }
}
