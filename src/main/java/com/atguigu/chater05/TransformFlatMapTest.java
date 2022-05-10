package com.atguigu.chater05;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author zengwang
 * @create 2022-05-08 10:48
 * @desc:
 */
public class TransformFlatMapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从元素中读取数据
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod", 3000L)
        );

        // 1. 使用自定义类，实现FlatMapFunction接口
        SingleOutputStreamOperator<String> result = stream.flatMap(new MyFlatMap());

        // 2. 使用lambda 表达式, 注意现在的函数参数给类型了
        SingleOutputStreamOperator<String> result2 = stream.flatMap((Event value, Collector<String> out) -> {
            // 灵活实现 一对一(map)、
            if (value.user.equals("Mary"))
                out.collect(value.url);
            else if (value.user.equals("Bob")) {
                // 一对多
                out.collect(value.user);
                out.collect(value.url);
                out.collect(value.timestamp.toString());
            }
            // 一对0(filter) 过滤了Alice
        }).returns(new TypeHint<String>() {});
        result.print("1");
        result2.print("2");
        env.execute();
    }

    // <输入, 输出>
    public static class MyFlatMap implements FlatMapFunction<Event, String> {
        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {
            out.collect(value.user);
            out.collect(value.url);
            out.collect(value.timestamp.toString());
        }
    }
}
