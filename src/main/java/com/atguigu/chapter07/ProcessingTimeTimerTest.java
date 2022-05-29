package com.atguigu.chapter07;

import com.atguigu.chapter05.ClickSource;
import com.atguigu.chapter05.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.expressions.E;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * @author zengwang
 * @create 2022-05-28 19:30
 * @desc:
 */
public class ProcessingTimeTimerTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 处理时间语义，不需要分配时间戳和watermark
        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        // 要用定时器必须基于KeyedStream
        stream.keyBy(data -> data.user)
                .process(new KeyedProcessFunction<String, Event, String>() {
                    @Override
                    public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx,
                                               Collector<String> out) throws Exception {
                        long curTs = ctx.timerService().currentProcessingTime();
                        out.collect(ctx.getCurrentKey() + "数据到达时间" + new Timestamp(curTs));
                        // 每来一条数据，注册一个10s的处理时间计时器(后面看结果，不是数据推着时间走，而是系统内部的时间到没到点)
                        ctx.timerService().registerProcessingTimeTimer(curTs + 10 * 1000L);
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx,
                                        Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        // timestamp为计时器到点触发的时间
                        out.collect(ctx.getCurrentKey() + "定器触发时间" + new Timestamp(timestamp));
                    }
                }).print();

        env.execute();
    }
}
