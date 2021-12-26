package com.study.flink.window;

import com.study.flink.model.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

public class FullWindowApiKafkaSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        String topic = "test_pi";
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.103.17.101:9092,10.103.17.102:9092,10.103.17.103:9092,10.103.17.104:9092");
        // 下面这些次要参数
        properties.setProperty("group.id", "flink");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");


        DataStream<String> dataStream = env.addSource(new FlinkKafkaConsumer011<String>(topic, new SimpleStringSchema(), properties));

        dataStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] strs = s.split(",");
                return new SensorReading(strs[0], Long.valueOf(strs[1]), Double.valueOf(strs[2]));
            }
            }).print();


        env.execute();
    }
}
