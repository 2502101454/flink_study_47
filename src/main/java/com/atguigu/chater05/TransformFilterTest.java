package com.atguigu.chater05;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zengwang
 * @create 2022-05-08 10:48
 * @desc:
 */
public class TransformFilterTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从元素中读取数据
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod", 3000L)
        );
        // Filter操作，通过布尔表达式设置过滤条件，对每个元素判断，如果为false，则过滤
        // 1. 使用自定义类，实现FilterFunction接口
        SingleOutputStreamOperator<Event> result = stream.filter(new MyFilter());

        // 2.使用匿名类实现FilterFunction接口
        SingleOutputStreamOperator<Event> result2 = stream.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event value) throws Exception {
                return value.user.equals("Bob");
            }
        });

        // 3.使用lambda表达式
        SingleOutputStreamOperator<Event> result3 = stream.filter(data -> data.user.equals("Alice"));

        result.print("impl filter");
        result2.print("匿名");
        result3.print("lambda");
        env.execute();
    }

    public static class MyFilter implements FilterFunction<Event> {
        @Override
        public boolean filter(Event value) throws Exception {
            return value.user.equals("Mary");
        }
    }
}
