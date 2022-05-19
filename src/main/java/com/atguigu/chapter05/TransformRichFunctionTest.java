package com.atguigu.chapter05;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zengwang
 * @create 2022-05-13 9:19 上午
 * @desc:
 */
public class TransformRichFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Bob", "./home", 3100L),
                new Event("Alice", "./profile", 3200L),
                new Event("Bob", "./prod?id=1", 3300L),
                new Event("steven", "./home", 3300L)
        );

        // 获取每个event 的url长度
        stream.map(new RichMapFunction<Event, Integer>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                System.out.println("索引为 " + getRuntimeContext().getIndexOfThisSubtask() + " 的任务开始");
            }

            @Override
            public void close() throws Exception {
                super.close();
                System.out.println("索引为 " + getRuntimeContext().getIndexOfThisSubtask() + " 的任务快结束");

            }

            @Override
            public Integer map(Event event) throws Exception {
                return event.url.length();
            }

        }).setParallelism(2)
                .print();

        env.execute();
    }
}
