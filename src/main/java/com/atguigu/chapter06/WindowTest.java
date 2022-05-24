package com.atguigu.chapter06;

import com.atguigu.chapter05.ClickSource;
import com.atguigu.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * @author zengwang
 * @create 2022-05-19 9:37
 * @desc:
 */
public class WindowTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 设置插入水位线的周期时间
        env.getConfig().setAutoWatermarkInterval(100);
        // 从元素中读取数据
//        DataStreamSource<Event> stream = env.fromElements(
//                new Event("Mary", "./home", 1000L),
//                new Event("Bob", "./cart", 2000L),
//                new Event("Alice", "./prod?id=100", 3000L),
//                new Event("Bob", "./home", 3100L),
//                new Event("Bob", "./prod?id=1", 3300L),
//                new Event("Alice", "./profile", 3200L)
//        );
        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        // 乱序流的watermark生成，不过这里测试数据都是有序的
        // 有序是乱序的一种延迟为0 的情况
        SingleOutputStreamOperator<Event> outOrderStream = stream.
                assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }));

        // 统计10s内，用户的点击次数
        outOrderStream.map(new MapFunction<Event, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(Event value) throws Exception {
                return Tuple2.of(value.user, 1L);
            }
        })
        .keyBy(data -> data.f0)
                // 滚动事件时间窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                // 滑动事件时间窗口
                // .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                // 事件时间会话窗口
                // .window(EventTimeSessionWindows.withGap(Time.seconds(2)))
                // 计数窗口
                // .countWindow(5)
                        .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                            @Override
                            public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                                return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                            }
                        }).print();

        // 基于处理时间的窗口，只需要修上面api中的event为process即可

        env.execute();
    }
}
