package com.atguigu.chapter06;

import com.atguigu.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author zengwang
 * @create 2022-05-24 8:32
 * @desc: 模拟乱序流中水位线，窗口延迟关闭
 */
public class WatermarkTest2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 将数据源改为socket文本流，并转为Event类型
        SingleOutputStreamOperator<Event> stream = env.socketTextStream("localhost", 7777)
                .map(new MapFunction<String, Event>() {
                    @Override
                    public Event map(String value) throws Exception {
                        String[] fields = value.split(",");
                        return new Event(fields[0].trim(), fields[1].trim(),
                                Long.valueOf(fields[2].trim()));
                    }
                })
                // 插入水位线逻辑，乱序流，设置延迟5s
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            // 抽取时间戳逻辑
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));
        stream.print("data-->");
        // 根据user分组
        stream.keyBy(data -> data.user)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new WatermarkTestResult())
                .print();

        env.execute();
    }

    // 自定义处理窗口函数，输出当前水位线和窗口的信息
    public static class WatermarkTestResult extends ProcessWindowFunction<Event, String, String, TimeWindow> {
        @Override
        public void process(String user, ProcessWindowFunction<Event, String, String, TimeWindow>.Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
            long start = context.window().getStart();
            long end = context.window().getEnd();
            long currentWatermark = context.currentWatermark();
            long count = elements.spliterator().getExactSizeIfKnown();
            out.collect(user + ", 窗口" + start + "~" + end + "中共有" + count +
                    "个元素， 窗口闭合计算时，水位线处于:"  + currentWatermark);
        }
    }

}
