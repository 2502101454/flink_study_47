package com.atguigu.chapter06;

import com.atguigu.chapter05.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zengwang
 * @create 2022-05-19 9:37
 * @desc:
 */
public class WatermarkTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 设置插入水位线的周期时间
        env.getConfig().setAutoWatermarkInterval(100);
        // 从元素中读取数据
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Bob", "./home", 3100L),
                new Event("Bob", "./prod?id=1", 3300L),
                new Event("Alice", "./profile", 3200L)
        );
    }
}
