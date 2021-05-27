package com.junying.day02;

import com.junying.bean.WaterSensor;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink08_Transform_RichMap
 *
 * @author King
 * @date 2021/5/24 18:28
 * @since 1.0.0
 */
public class Flink08_Transform_RichMap {
    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从文件读取数据
        DataStreamSource<String> stringDataStreamSource = env.readTextFile("Flink2021/input/sensor.txt");

        //3.将每行数据转换为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = stringDataStreamSource.map(new MyRichMapFunc());

        //4.打印结果
        waterSensorDS.print();

        //5.执行任务
        env.execute();
    }

    //RichFunction富有的地方在于：1.声明周期方法 2.可以获取上下文环境做状态编程
    public static class MyRichMapFunc extends RichMapFunction<String, WaterSensor> {

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("MyRichMapFunc.open方法被调用！！");
        }

        @Override
        public WaterSensor map(String value) throws Exception {
            String[] split = value.split(",");
            return new WaterSensor(
                    split[0],
                    Long.parseLong(split[1]),
                    Integer.parseInt(split[2])
            );
        }

        @Override
        public void close() throws Exception {
            System.out.println("MyRichMapFunc.close方法被调用！！");
        }


    }

}
