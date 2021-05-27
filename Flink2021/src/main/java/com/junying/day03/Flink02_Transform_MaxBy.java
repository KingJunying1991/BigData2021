package com.junying.day03;

import com.junying.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink02_Transform_MaxBy
 *
 * @author King
 * @date 2021/5/24 23:26
 * @since 1.0.0
 */
public class Flink02_Transform_MaxBy {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.读取端口数据并转换为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("hadoop102", 9999)
                .map(value -> {
                    String[] split = value.split(",");
                    return new WaterSensor(
                            split[0],
                            Long.parseLong(split[1]),
                            Integer.parseInt(split[2])
                    );
                });

        //3.按照传感器id分组
        KeyedStream<WaterSensor, String> keyedStream = waterSensorDS.keyBy(value -> value.getId());

        //4.计算最高水位线
        SingleOutputStreamOperator<WaterSensor> result = keyedStream.maxBy("vc", false);

        //5.打印
        result.print();

        //6.执行任务
        env.execute();
    }
}
