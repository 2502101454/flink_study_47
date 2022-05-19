package com.atguigu.chapter05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zengwang
 * @create 2022-05-08 10:48
 * @desc:
 */
public class TransformMapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从元素中读取数据
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod", 3000L)
        );

        // 进行转换计算，提取user字段
        // 1. 使用自定义类，实现MapFunction接口
//        SingleOutputStreamOperator<String> result = stream.map(new MyMapper());

        // 2.使用匿名类实现MapFunction接口
        SingleOutputStreamOperator<String> result1 = stream.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event value) throws Exception {
                return value.user;
            }
        });

        // 3.使用lambda表达式
        SingleOutputStreamOperator<String> result2 = stream.map(data -> data.user);
        result2.print();
        env.execute();
    }

    // 自定义MapFunction, <输入， 输出>
    public static class MyMapper implements MapFunction<Event, String> {
        @Override
        public String map(Event value) throws Exception {
            return value.user;
        }
    }
}
