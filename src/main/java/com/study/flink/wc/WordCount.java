package com.study.flink.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCount {
    public static void main(String[] args) throws Exception {
        //执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //文件中读取数据
        String inputPath = "/Users/piguanghua/My_Github_Repository/StudyFlink/src/main/resources/hello.txt";
        DataSource<String> dataSource = env.readTextFile(inputPath);

        //对数据集进行处理，按空格分词展开，转换成(word, 1)二元组进行统计

        DataSet<Tuple2<String, Integer>> resultSet = dataSource.flatMap(new MyFlatMapper()).groupBy(0).sum(1);

        resultSet.print();

    }

    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>>{

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] words = s.split(" ");
            for(String word:words){
                collector.collect(new Tuple2<>(word, 1));
            }

        }
    }
}
