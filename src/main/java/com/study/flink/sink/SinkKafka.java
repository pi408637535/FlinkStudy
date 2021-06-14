package com.study.flink.sink;

import com.study.flink.model.SensorReading;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

public class SinkKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> dataStream = env.addSource(new SourceFunction<String>(){

            private boolean flag = true;

            @Override
            public void run(SourceContext<String> sourceContext) throws Exception {
                int i = 0;
                while (flag){
                    while (i < 20){
                        sourceContext.collect(new SensorReading("sensor1", 1623230417L,36.2).toString());
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
        String broker = "10.103.17.101:9092,10.103.17.102:9092,10.103.17.103:9092";
        String topic = "test_pi";
        // 打印输出
        dataStream.addSink(new FlinkKafkaProducer011<String>(broker, topic, new SimpleStringSchema()));

        env.execute();
    }
}
