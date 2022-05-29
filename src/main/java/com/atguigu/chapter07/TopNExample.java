package com.atguigu.chapter07;

import com.atguigu.chapter05.ClickSource;
import com.atguigu.chapter05.Event;
import com.atguigu.chapter06.UrlViewCount;
import com.atguigu.chapter06.UrlViewCountExample;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * @author zengwang
 * @create 2022-05-29 12:37
 * @desc:
 */
public class TopNExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
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
        outOrderStream.print("source");
        // 1. 按照url分组，统计窗口内每个url的访问量
        SingleOutputStreamOperator<UrlViewCount> urlCountStream = outOrderStream.keyBy(data -> data.url)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new UrlViewCountExample.UrlViewCountAgg(),
                        new UrlViewCountExample.UrlViewCountResult());

        urlCountStream.print("url count");

        // 2. 对于同一窗口统计出的访问量，进行收集和排序
        SingleOutputStreamOperator<String> result = urlCountStream.keyBy(data -> data.windowEnd)
                .process(new TopN(2));

        result.print("result");
        env.execute();

    }
    // 实现自定义的KeyedProcessFunction
    public static class TopN extends KeyedProcessFunction<Long, UrlViewCount, String>{
        // 将n作为属性
        private Integer n;
        // 定义一个列表状态
        private ListState<UrlViewCount> urlViewCountListState;

        public TopN(Integer n) {
            this.n = n;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // 从环境中获取列表状态句柄
            urlViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<UrlViewCount>("url-view-count-list",
                    Types.POJO(UrlViewCount.class)));
        }

        @Override
        public void processElement(UrlViewCount value,
                                   KeyedProcessFunction<Long, UrlViewCount, String>.Context ctx,
                                   Collector<String> out) throws Exception {
            // 将count数据添加到列表状态中
            urlViewCountListState.add(value);
            out.collect("value: " + value + "currentWatermark : " +
                    new Timestamp(ctx.timerService().currentWatermark()));
            // 注册window end + 1ms后的定时器，等待所有数据到齐开始排序
            ctx.timerService().registerEventTimeTimer(ctx.getCurrentKey() + 1L);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, UrlViewCount, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect( "currentWatermark : " + new Timestamp(ctx.timerService().currentWatermark()));

            // 将数据从列表状态取出来，放入ArrayList方便排序
            ArrayList<UrlViewCount> urlViewCountArrayList = new ArrayList<>();
            for (UrlViewCount urlViewCount: urlViewCountListState.get()) {
                urlViewCountArrayList.add(urlViewCount);
            }

            // 清空状态，释放资源
            urlViewCountListState.clear();

            // 排序
            urlViewCountArrayList.sort(new Comparator<UrlViewCount>() {
                @Override
                public int compare(UrlViewCount o1, UrlViewCount o2) {
                    return o2.count.intValue() - o1.count.intValue();
                }
            });

            // 取前topN，构建输出结果
            StringBuilder result = new StringBuilder();
            result.append("---------------------------------");
            // 因为上面设置的计时器时间是窗口结束时间 + 1
            result.append("窗口结束时间: " + new Timestamp(timestamp - 1) + "\n");

            // 这个程序如果10s内一直都是一种url，那么hashMap只有一个元素，这里也就越界了哈
            for (int i = 0; i < this.n; i++) {
                UrlViewCount urlViewCount = urlViewCountArrayList.get(i);

                String info = "No. " + (i + 1) + " url: " + urlViewCount.url
                        + " 浏览量: " + urlViewCount.count + "\n";
                result.append(info);
            }
            result.append("===================================");
            out.collect(result.toString());
        }
    }
}
