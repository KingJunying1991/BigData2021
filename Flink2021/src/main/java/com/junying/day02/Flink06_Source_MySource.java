package com.junying.day02;

import com.junying.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * Flink06_Source_MySource
 *
 * @author King
 * @date 2021/5/21 16:33
 * @since 1.0.0
 */
public class Flink06_Source_MySource {
    public static void main(String[] args) throws Exception {

        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从自定义的数据源中加载数据
        DataStreamSource<WaterSensor> objectDataStreamSource = env.addSource(new MySource("hadoop102",9999));

        //3.打印结果
        objectDataStreamSource.print();

        //4.执行任务
        env.execute();
    }

    //自定义从端口读取数据的Source
    public static class MySource implements SourceFunction<WaterSensor> {

        //定义属性信息，主机&端口
        private String host;
        private Integer port;

        public MySource() {
        }

        public MySource(String host, Integer port) {
            this.host = host;
            this.port = port;
        }

        Socket socket = null;
        BufferedReader reader = null;

        private Boolean running = true;

        @Override
        public void run(SourceContext<WaterSensor> ctx) throws Exception {
            //创建输入流
            socket = new Socket(host,port);
            reader = new BufferedReader(new InputStreamReader(socket.getInputStream(),
                    StandardCharsets.UTF_8));
            //读取数据
            String line = reader.readLine();

            while( running && line != null) {

                //接收数据并发送至Flink系统
                String[] split = line.split(",");
                WaterSensor waterSensor = new WaterSensor(
                        split[0],
                        Long.parseLong(split[1]),
                        Integer.parseInt(split[2])
                );
                ctx.collect(waterSensor);
                line = reader.readLine();

            }

        }

        @Override
        public void cancel() {
            running = false;
            try {
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}

