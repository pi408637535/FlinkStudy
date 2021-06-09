package com.study.flink.source;

import com.study.flink.model.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class SourceCollection {
    public static void main(String[] args) throws Exception {
        //执行环境

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<SensorReading> dataStreamSource = env.fromCollection(Arrays.asList(
                new SensorReading("sensor1", 1623230417L,36.2),
                new SensorReading("sensor2", 1623230417L,36.3),
                new SensorReading("sensor3", 1623230417L,36.4)
                ));
        dataStreamSource.print();
        env.execute();
    }
}
