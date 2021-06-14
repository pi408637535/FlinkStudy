package com.study.flink.window;

import com.study.flink.model.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

public class WindowApiDemo2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        String hostname = "localhost";
        int port = 7777;
        DataStream<String> dataStream =  env.socketTextStream(hostname, port);

        dataStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] strs = s.split(",");
                return new SensorReading(strs[0], Long.valueOf(strs[1]), Double.valueOf(strs[2]));
            }
        }).keyBy("name")
                .timeWindow(Time.seconds(2L))
            .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {

                @Override
                public Integer createAccumulator() {
                    return 0;
                }

                @Override
                public Integer add(SensorReading sensorReading, Integer integer) {
                    return integer + 1;
                }

                @Override
                public Integer getResult(Integer integer) {
                    return integer;
                }

                @Override
                public Integer merge(Integer integer, Integer acc1) {
                    return integer + acc1;
                }
            }).print();


        env.execute();
    }
}
