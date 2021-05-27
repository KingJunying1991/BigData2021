package com.junying.day03;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink06_Tramsform_Repartition
 *
 * @author King
 * @date 2021/5/26 20:57
 * @since 1.0.0
 */
public class Flink06_Tramsform_Repartition {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(5);

        //2.读取端口数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //3.使用不同的重分区策略分区后打印
//        socketTextStream.keyBy(data -> data).print("KeyBy");
//        socketTextStream.shuffle().print("Shuffle");
//        socketTextStream.rebalance().print("Rebalance");
//       socketTextStream.rescale().print("Rescale");
//       socketTextStream.global().print("Global");
//        socketTextStream.forward().print("Forward");
        socketTextStream.broadcast().print("Broadcast");

        //4.开启任务
        env.execute();
    }
}
