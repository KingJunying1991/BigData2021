package com.junying.day04;

import com.junying.bean.UserBehavior;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;

/**
 * Flink05_Practice_UserVisitor
 *
 * @author King
 * @date 2021/5/27 20:19
 * @since 1.0.0
 */
public class Flink05_Practice_UserVisitor {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.读取文本数据
        DataStreamSource<String> readTextFile = env.readTextFile("Flink2021/input/UserBehavior.csv");

        //3.转换为JavaBean并过滤出PV的数据
        SingleOutputStreamOperator<UserBehavior> userBehaviorDS = readTextFile.flatMap((String data, Collector<UserBehavior> out) -> {
            //a.按照“，”分割
            String[] split = data.split(",");
            //b.分装JavaBean对象
            UserBehavior userBehavior = new UserBehavior(
                    Long.parseLong(split[0]),
                    Long.parseLong(split[1]),
                    Integer.parseInt(split[2]),
                    split[3],
                    Long.parseLong(split[4])
            );
            //c.选择需要输出的数据
            if ("pv".equals(userBehavior.getBehavior())) {
                out.collect(userBehavior);
            }
        }).returns(TypeInformation.of(UserBehavior.class));

        //4.指定key分组
        KeyedStream<UserBehavior, String> keyedStream = userBehaviorDS.keyBy(data -> "UV");

        //5.使用Process方式计算总和(注意UserId的去重)
        SingleOutputStreamOperator<Integer> result = keyedStream.process(new KeyedProcessFunction<String, UserBehavior, Integer>() {
            private Integer count = 0;
            private HashSet<Long> uids = new HashSet<>();

            @Override
            public void processElement(UserBehavior value, Context ctx, Collector<Integer> out) throws Exception {
                if (!uids.contains(value.getUserId())) {
                    uids.add(value.getUserId());
                    count++;
                    out.collect(count);
                }
            }
        });


        //6.打印输出
        result.print();

        //7.执行任务
        env.execute();

    }
}
