package com.study.flink.project;

import com.alibaba.fastjson.JSON;
import com.study.flink.model.GlobalBand;
import com.study.flink.model.SensorReading;
import com.study.flink.utils.OkHttpUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class WordCountStat {
    final static OkHttpUtil okHttpUtil = OkHttpUtil.getOkHttpUtil();

    public static void main(String[] args) throws Exception {
        final String URL_TOKEN = "http://lc1.haproxy.yidian.com:8040/StoneCutter/Handler?text=";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String topic = "cpp_trace_test_pi";
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.103.17.101:9092,10.103.17.102:9092,10.103.17.103:9092,10.103.17.104:9092");
        // 下面这些次要参数
        properties.setProperty("group.id", "cpp-test");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        DataStream<String> dataStream = env.addSource(new FlinkKafkaConsumer011<String>(topic, new SimpleStringSchema(), properties));
        dataStream.map(new MapFunction<String, GlobalBand>() {
            @Override
            public GlobalBand map(String s) throws Exception {
                return JSON.parseObject(s, GlobalBand.class);
            }
        }).flatMap(new FlatMapFunction<GlobalBand, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(GlobalBand globalBand, Collector<Tuple2<String, Integer>> collector) throws Exception {
               String tokens = okHttpUtil.get(URL_TOKEN + globalBand.getBand());
//                String tokens = globalBand.toString();
               for(String token : tokens.split(" ")){
                   collector.collect(new Tuple2<>(token, 1));
               }
            }
        }).keyBy(0).timeWindow(Time.seconds(2)).sum(1).print();


        env.execute();
    }
}
