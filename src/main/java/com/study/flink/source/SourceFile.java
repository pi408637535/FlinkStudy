package com.study.flink.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SourceFile {
    public static void main(String[] args) throws Exception {
        //执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String inputPath = "/Users/admin/My_Git_Example/StudyFlink/src/main/resources/hello.txt";
        DataStreamSource<String> dataSource = env.readTextFile(inputPath);

        dataSource.print();
        env.execute();
    }
}
