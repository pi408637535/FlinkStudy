package com.study.flink.partition;

import com.study.flink.model.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class PartitionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        DataStreamSource<SensorReading> dataStream = env.addSource(new SourceFunction<SensorReading>(){

            private boolean flag = true;

            @Override
            public void run(SourceContext<SensorReading> sourceContext) throws Exception {
                int i = 0;
                while (flag){
                    while (i < 6){
                        sourceContext.collect(new SensorReading(String.format("sensor%d",i), 1623230417L,36.2));
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
//        dataStream.print();

        //global
//        System.out.println("global");
//        dataStream.global().print();

        //rebalanced
//        System.out.println("rebalanced");
//        dataStream.rebalance().print();

        //rescale
        System.out.println("rebalanced");
        dataStream.rescale().print();
        env.execute();
    }
}
