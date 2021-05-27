package com.junying.day02;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * Fink10_Transform_RichFilter
 *
 * @author King
 * @date 2021/5/24 18:48
 * @since 1.0.0
 */
public class Fink10_Transform_RichFilter {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从文件读取数据
        DataStreamSource<String> textFile = env.readTextFile("Flink2021/input/sensor.txt");

        //3.过滤数据，只取水位高于30的
        SingleOutputStreamOperator<String> result = textFile.filter(new MyRichFilterFunc());

        //4.打印结果
        result.print();

        //5.执行任务
        env.execute();
    }

    public static class MyRichFilterFunc extends RichFilterFunction<String> {

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("MyRichFilterFunc被调用");
        }

        @Override
        public boolean filter(String value) throws Exception {
            String[] split = value.split(",");
            return Integer.parseInt(split[2]) > 30;
        }
    }
}
