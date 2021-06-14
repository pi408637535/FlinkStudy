package com.study.flink.udf;

import com.study.flink.model.SensorReading;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class RichFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStreamSource<SensorReading> dataStream = env.addSource(new SourceFunction<SensorReading>(){

            private boolean flag = true;

            @Override
            public void run(SourceContext<SensorReading> sourceContext) throws Exception {
                int i = 0;
                while (flag){
                    while (i < 5){
                        sourceContext.collect(new SensorReading("sensor1", 1623230417L,36.2 + i));
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
        dataStream.map(new MyRichFunction()).print();

        env.execute();
    }

    static class MyRichFunction extends RichMapFunction<SensorReading, Boolean>{

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            System.out.println("open");
        }

        @Override
        public void close() throws Exception {
            super.close();
            System.out.println("close");
        }

        @Override
        public Boolean map(SensorReading sensorReading) throws Exception {
            if(sensorReading.getTemperature() > 38){
                return true;
            }else{
                return false;
            }
        }
    }
}
