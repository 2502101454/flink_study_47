package com.atguigu.chater05;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zengwang
 * @create 2022-05-10 9:35
 * @desc:
 */
public class TransformSimpleAggTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从元素中读取数据
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Bob", "./home", 3100L),
                new Event("Bob", "./prod?id=1", 3300L),
                new Event("Alice", "./profile", 3200L)
        );

        // 按键分组之后进行聚合,提取当前用户最后一次访问数据，<输入数据类型，key的类型>
        KeyedStream<Event, String> keyedStream = stream.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event value) throws Exception {
                return value.user;
            }
        });
        // Pojo类，聚合函数max 传字段名称
        SingleOutputStreamOperator<Event> maxTime = keyedStream.max("timestamp");
        maxTime.print("max");
        env.execute();
    }
}
