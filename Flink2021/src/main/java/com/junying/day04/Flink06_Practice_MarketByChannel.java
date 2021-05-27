package com.junying.day04;

import com.junying.bean.MarketingUserBehavior;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Flink06_Practice_MarketByChannel
 *
 * @author King
 * @date 2021/5/27 21:01
 * @since 1.0.0
 */
public class Flink06_Practice_MarketByChannel {

    public static void main(String[] args) throws Exception {

            //1.创建执行环境
            StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.getExecutionEnvironment();
            //env.setParallelism(1);

            //2.从自定义Source中加载数据
        DataStreamSource<MarketingUserBehavior> marketingUserBehaviorDS = env
                .addSource(new AppMarketingDataSource());

        //3.按照渠道以及行为分组


            //4.计算总和


            //5.打印

            //6.执行任务
            env.execute();
        }

    public static class AppMarketingDataSource extends RichSourceFunction<MarketingUserBehavior> {

        boolean canRun = true;
        Random random = new Random();
        List<String> channels = Arrays.asList("huawwei", "xiaomi", "apple", "baidu", "qq", "oppo", "vivo");
        List<String> behaviors = Arrays.asList("download", "install", "update", "uninstall");

        @Override
        public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
            while(canRun) {
                MarketingUserBehavior marketingUserBehavior = new MarketingUserBehavior(
                        (long) random.nextInt(1000000),
                        behaviors.get(random.nextInt(behaviors.size())),
                        channels.get(random.nextInt(channels.size())),
                        System.currentTimeMillis()
                );
                ctx.collect(marketingUserBehavior);
                Thread.sleep(1000);
            }
        }


        @Override
        public void cancel() {
            canRun = false;
        }
    }
}
