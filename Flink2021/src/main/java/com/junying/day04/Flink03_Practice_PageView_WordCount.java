package com.junying.day04;

import com.junying.bean.UserBehavior;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Flink03_Practice_PageView_WordCount
 *
 * @author King
 * @date 2021/5/26 21:46
 * @since 1.0.0
 */
public class Flink03_Practice_PageView_WordCount {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        //2.读取文本文件
        DataStreamSource<String> readTextFile = env
                .readTextFile("flink2021/input/UserBehavior.csv");

        //3.转换为JavaBean并过滤出PV的数据
        SingleOutputStreamOperator<Tuple2<String, Integer>> pv = readTextFile
                .flatMap((String data, Collector<Tuple2<String, Integer>> out) -> {
                    String[] split = data.split(",");
                    UserBehavior userBehavior = new UserBehavior(
                            Long.parseLong(split[0]),
                            Long.parseLong(split[1]),
                            Integer.parseInt(split[2]),
                            split[3],
                            Long.parseLong(split[4])
                    );

                    //选出需要输出的数据
                    if ("pv".equals(userBehavior.getBehavior())) {
                        out.collect(new Tuple2<>("pv", 1));
                    }

                }).returns(Types.TUPLE(Types.STRING, Types.INT));

        //4.指定key分组
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = pv.keyBy(data -> data.f0);

        //5.计算总和
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);

        //6.打印数据
        result.print();

        //7.执行任务
        env.execute();
    }
}
