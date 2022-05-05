package com.atguigu.chater05;

import org.apache.flink.api.common.typeutils.base.EnumSerializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * @author zengwang
 * @create 2022-05-05 9:20
 * @desc:
 */
public class SourceTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.从文件中读取数据
        DataStreamSource<String> stream1 = env.readTextFile("input/clicks.txt");

        // 2.从集合中读取数据
        ArrayList<Integer> nums = new ArrayList<>();
        nums.add(2);
        nums.add(5);
        DataStreamSource<Integer> numStream = env.fromCollection(nums);

        ArrayList<Event> events = new ArrayList<>();
        events.add(new Event("mary", "./home", 1000L));
        events.add(new Event("bob", "./cart", 2000L));
        DataStreamSource<Event> eventsStream = env.fromCollection(events);

        // 3.从元素中读取
        DataStreamSource<Event> eventsStream2 = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        // 给打印的内容加上前缀
        stream1.print("1");
        numStream.print("nums");
        // 调event对象的toString方法
        eventsStream.print("events");
        eventsStream2.print("events2");

        env.execute();

    }
}
