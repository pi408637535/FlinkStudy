package com.study.flink.wc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //文件中读取数据
        String inputPath = "/Users/piguanghua/My_Github_Repository/StudyFlink/src/main/resources/hello.txt";
        DataStreamSource<String> dataSource = env.readTextFile(inputPath);

        //对数据集进行处理，按空格分词展开，转换成(word, 1)二元组进行统计

        DataStream<Tuple2<String, Integer>> resultStream = dataSource.flatMap(new WordCount.MyFlatMapper()).keyBy(0).sum(1);

        resultStream.print();

        //执行任务
        env.execute();
    }
}
