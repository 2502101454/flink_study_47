package com.atguigu.chapter07;

import com.atguigu.chapter05.ClickSource;
import com.atguigu.chapter05.Event;
import com.atguigu.chapter06.UrlViewCount;
import com.atguigu.chapter06.UrlViewCountExample;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.*;

/**
 * @author zengwang
 * @create 2022-05-29 12:37
 * @desc:
 */
public class TopNExample_ProcessAllWindowFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        stream.print("data--");
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
        // 只需要url就可统计数量，所以转换成String直接开窗统计
        SingleOutputStreamOperator<String> urlStream = outOrderStream.map(data -> data.url);
        SingleOutputStreamOperator<String> result = urlStream.windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new UrlHashMapCountAgg(), new UrlAllWindowResult());

        result.print();
        env.execute();
    }
    // 实现自定义的增量聚合函数
    public static class UrlHashMapCountAgg implements AggregateFunction<String, HashMap<String, Long>,
            ArrayList<Tuple2<String, Long>>> {
        @Override
        public HashMap<String, Long> createAccumulator() {
            return new HashMap<>();
        }

        @Override
        public HashMap<String, Long> add(String value, HashMap<String, Long> accumulator) {
            if (accumulator.containsKey(value)) {
                Long count = accumulator.get(value);
                accumulator.put(value, count + 1L);
            } else {
                accumulator.put(value, 1L);
            }
            return accumulator;
        }

        @Override
        public ArrayList<Tuple2<String, Long>> getResult(HashMap<String, Long> accumulator) {
            ArrayList<Tuple2<String, Long>> result = new ArrayList<>();
            // 将浏览量数据放入ArrayList中进行排序，降序
            for (String key: accumulator.keySet()) {
                result.add(Tuple2.of(key, accumulator.get(key)));
            }
            result.sort(new Comparator<Tuple2<String, Long>>() {
                @Override
                public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
                    return o2.f1.intValue() - o1.f1.intValue();
                }
            });
            return result;
        }

        @Override
        public HashMap<String, Long> merge(HashMap<String, Long> a, HashMap<String, Long> b) {
            return null;
        }
    }
    // 实现自定义的函数
    public static class UrlAllWindowResult extends ProcessAllWindowFunction<
            ArrayList<Tuple2<String, Long>>, String, TimeWindow> {
        @Override
        public void process(ProcessAllWindowFunction<ArrayList<Tuple2<String, Long>>, String, TimeWindow>.Context context,
                            Iterable<ArrayList<Tuple2<String, Long>>> elements,
                            Collector<String> out) throws Exception {
            StringBuilder result = new StringBuilder();
            result.append("---------------------------------");
            result.append("窗口结束时间: " + new Timestamp(context.window().getEnd()) + "\n");

            // 取List前两个，包装信息输出
            ArrayList<Tuple2<String, Long>> list = elements.iterator().next();
            // 这个程序如果10s内一直都是一种url，那么hashMap只有一个元素，这里也就越界了哈
            for (int i = 0; i < 2; i++) {
                Tuple2<String, Long> currTuple = list.get(i);
                String info = "No. " + (i + 1) + " url: " + currTuple.f0 + " 访问量: " + currTuple.f1 + "\n";
                result.append(info);
            }
            out.collect(result.toString());
        }
    }
}
