package com.atguigu.chapter07;

import com.atguigu.chapter05.ClickSource;
import com.atguigu.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * @author zengwang
 * @create 2022-05-28 20:29
 * @desc:
 */
public class EventTimeTimerTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // DataStreamSource<Event> stream = env.addSource(new ClickSource());
        DataStreamSource<Event> stream = env.addSource(new CustomSource());

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

        outOrderStream.keyBy(data -> data.user)
                .process(new KeyedProcessFunction<String, Event, String>() {
                    @Override
                    public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx,
                                               Collector<String> out) throws Exception {
                        // 之前的ProcessFunctionTest demo中测试，也是在processElement中:
                        // 发现watermark起初是一个极小的负数
                        // 然后还会又延迟，总是当前数据接受到了之后，才会打印上一个数据的watermark

                        // ctx.timestamp() 获取事件时间语义下，事件自身的时间戳
                        long curTs = ctx.timestamp();
                        out.collect(ctx.getCurrentKey() + "数据时间戳" + new Timestamp(curTs)
                        + " watermark: " + ctx.timerService().currentWatermark());
                        // 每来一条数据，注册一个10s的事件时间计时器
                        ctx.timerService().registerEventTimeTimer(curTs + 10 * 1000L);
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx,
                                        Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        // timestamp为计时器到点触发的时间
                        out.collect(ctx.getCurrentKey() + "定器触发时间" + new Timestamp(timestamp)
                        +" watermark: " + ctx.timerService().currentWatermark());
                    }
                }).print();

        env.execute();
    }

    // 自定义测试数据源
    public static class CustomSource implements SourceFunction<Event> {
        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            // 直接发出测试数据
            ctx.collect(new Event("Mary", "./home", 1000L));
            // 为了更加明显，中间停顿5s
            Thread.sleep(5000L);

            // 发出10s后的数据，此时水位线是10999，不会触发1000L的10s的计时器的
            ctx.collect(new Event("Mary", "./home", 11000L));
            Thread.sleep(5000L);

            // 发出10s + 1ms的数据，让水位线为11000ms。则触发10s的计时器
             ctx.collect(new Event("Alice", "./cart", 11001L));
             Thread.sleep(5000L);

            // 最后没有数据的话，Flink会直接将水位线推到正的很大的数，触发所有的计时器服务
        }

        @Override
        public void cancel() {

        }
    }
}
