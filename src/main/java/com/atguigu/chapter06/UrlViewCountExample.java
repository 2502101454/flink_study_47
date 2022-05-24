package com.atguigu.chapter06;

import com.atguigu.chapter05.ClickSource;
import com.atguigu.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.jasper.tagplugins.jstl.core.Url;

import java.time.Duration;

/**
 * @author zengwang
 * @create 2022-05-23 13:55
 * @desc:
 */
public class UrlViewCountExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 设置插入水位线的周期时间
        env.getConfig().setAutoWatermarkInterval(100);

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

        outOrderStream.print("data-->");

        // 每5s钟，计算最近10s内的每个url的访问量，并结合窗口信息进行输出
        outOrderStream.keyBy(data -> data.url)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new UrlViewCountAgg(), new UrlViewCountResult())
                .print();
        env.execute();
    }
    // 自定义增量聚合函数
    public static class UrlViewCountAgg implements AggregateFunction<Event, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Event value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }

    // 自定义窗口处理函数，包装窗口信息进行输出
    public static class UrlViewCountResult extends ProcessWindowFunction
            <Long, UrlViewCount, String, TimeWindow> {
        @Override
        public void process(String url, ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow>.Context context,
                            Iterable<Long> elements, Collector<UrlViewCount> out) throws Exception {
            // String url 是key的值
            long start = context.window().getStart();
            long end = context.window().getEnd();
            // 迭代器中只有一个元素，就是增量聚合的结果
            out.collect(new UrlViewCount(url, elements.iterator().next(), start, end));
        }
    }

}
