package com.study.flink.transformer;

import com.study.flink.model.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class BaseTransformer {
    public static void main(String[] args) throws Exception {
        //执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String inputPath = "/Users/admin/My_Git_Example/StudyFlink/src/main/resources/hello.txt";
        DataStreamSource<String> dataSource = env.readTextFile(inputPath);

        dataSource.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String s) throws Exception {
                return s.length();
            }
        });


         env.execute();
    }
}
