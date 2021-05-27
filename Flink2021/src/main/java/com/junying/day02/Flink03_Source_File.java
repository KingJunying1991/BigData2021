package com.junying.day02;

import com.junying.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink03_Source_File
 *
 * @author King
 * @date 2021/5/21 12:27
 * @since 1.0.0
 */
public class Flink03_Source_File {
    public static void main(String[] args) throws Exception {

        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从文件读取数据
        DataStreamSource<String> stringDataStreamSource =
                env.readTextFile("Flink2021/input/sensor.txt");

        //3.转换为JavaBean并打印数据
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = stringDataStreamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(
                        split[0],
                        Long.parseLong(split[1]),
                        Integer.parseInt(split[2])
                );
            }
        });
        waterSensorDS.print();

        //4.执行
        env.execute();
    }
}
