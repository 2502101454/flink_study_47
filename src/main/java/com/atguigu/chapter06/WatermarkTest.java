package com.atguigu.chapter06;

import com.atguigu.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * @author zengwang
 * @create 2022-05-19 9:37
 * @desc:
 */
public class WatermarkTest {
    public static void main(String[] args) throws Exception {
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

        // 有序流的watermar生成
        SingleOutputStreamOperator<Event> withWatermarks = stream
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        // 提取数据中的时间戳字段，第二个参数对应，如果输入的数据本身单独带时间戳信息，比如kafka
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            // 要求毫秒单位
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );
        // 乱序流的watermark生成，设置延迟时间2s
        SingleOutputStreamOperator<Event> outOrderWithWatermarks = stream.
                assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }));

        env.execute();
    }
}
