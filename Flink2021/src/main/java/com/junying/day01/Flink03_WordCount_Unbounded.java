package com.junying.day01;

import com.junying.day01.Flink02_WordCount_Bounded.LineToTupleFlatMapFunc;


/**
 * Flink03_WordCount_Unbounded
 *
 * @author King
 * @date 2021/5/20 20:04
 * @since 1.0.0
 */
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 关于并行度优先级问题：
 * 1.代码中算子单独设置
 * 2.代码中Env全局设置
 * 3.提交参数
 * 4.默认配置信息
 */
public class Flink03_WordCount_Unbounded {

    public static void main(String[] args) throws Exception {

        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        //2.读取端口数据创建流
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //3.将每行数据压平并转换为元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDS = socketTextStream.flatMap(new LineToTupleFlatMapFunc());


        //4.分组
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordToOneDS.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        //5.聚合结果
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);

        //6.打印结果
        result.print("Result");

        //7.启动任务
        env.execute();

    }
}
