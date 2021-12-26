package com.study.flink.window;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.support.hsf.HSFJSONUtils;
import com.study.flink.model.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.util.JSONPObject;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

public class EventTimeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String topic = "cpp_trace_test_pi";
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.103.17.101:9092,10.103.17.102:9092,10.103.17.103:9092,10.103.17.104:9092");
        // 下面这些次要参数
        properties.setProperty("group.id", "cpp-test");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        /**
         *
         * .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(1L)) {
         *             @Override
         *             public long extractTimestamp(SensorReading element) {
         *                 return element.getTimestap();
         *             }
         *         })
         * */
        DataStream<String> dataStream = env.addSource(new FlinkKafkaConsumer011<String>(topic, new SimpleStringSchema(), properties));

        dataStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                SensorReading sensorReading = JSON.parseObject(s, SensorReading.class);
//                String[] strs = s.split(",");
                return sensorReading;
            }
        }).keyBy("name").timeWindow(Time.seconds(1)).allowedLateness(Time.seconds(1))
                .minBy("temperature").print();

        env.execute();
    }
}
