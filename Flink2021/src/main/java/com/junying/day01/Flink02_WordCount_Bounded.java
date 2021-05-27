package com.junying.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Flink02_WordCount_Bounded
 *
 * @author King
 * @date 2021/5/20 14:01
 * @since 1.0.0
 */
public class Flink02_WordCount_Bounded {
    public static void main(String[] args) throws Exception {

        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.读取文件创建流
        DataStreamSource<String> input = env.readTextFile("Flink2021/input/word.txt");

        //3.压平并将单词转换为元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDS = input.flatMap(new LineToTupleFlatMapFunc());

        //4.分组
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordToOneDS.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        //5.按照key做聚合操作
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);

        //6.打印结果
        result.print();

        //7.启动任务
        env.execute("Flink02_WordCount_Bounded");

    }

    public static class LineToTupleFlatMapFunc implements FlatMapFunction<String,Tuple2<String,Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            //1.按照空格进行切分
            String[] words = value.split(" ");
            //2.遍历写出
            for (String word:words
                 ) {
                    out.collect(new Tuple2<>(word,1));
            }
        }
    }
}
