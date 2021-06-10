package com.study.flink.source;

import com.study.flink.model.SensorReading;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

public class SourceMySelf {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);




        DataStreamSource<SensorReading> dataStream = env.addSource(new SourceFunction<SensorReading>(){

            private boolean flag = true;

            @Override
            public void run(SourceContext<SensorReading> sourceContext) throws Exception {
                int i = 0;
                while (flag){
                    while (i < 5){
                        sourceContext.collect(new SensorReading("sensor1", 1623230417L,36.2));
                        i++;
                    }
                    flag = false;
                }
            }

            @Override
            public void cancel() {
                flag = false;
            }
        });

        // 打印输出
        dataStream.print();

        env.execute();
    }
}
