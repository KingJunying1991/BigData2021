package com.junying.day02;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * Flink05_Source_Kafka
 *
 * @author King
 * @date 2021/5/21 16:12
 * @since 1.0.0
 */
public class Flink05_Source_Kafka {
    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从kafka读取数据
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"Flink2021");
        DataStreamSource<String> kafkaDS =
                env
                        .addSource(
                                new FlinkKafkaConsumer<String>(
                                        "test",
                                        new SimpleStringSchema(),
                                        properties));

        //3.将数据打印
        kafkaDS.print();

        //4.执行任务
        env.execute();
    }
}
