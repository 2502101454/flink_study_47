package com.atguigu.chapter07;

import com.atguigu.chapter05.ClickSource;
import com.atguigu.chapter05.Event;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author zengwang
 * @create 2022-05-30 9:41
 * @desc:
 */
public class SideOutputTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        // 定义侧输出流
        OutputTag<String> outputTag = new OutputTag<String>("side-output") {};

        SingleOutputStreamOperator<String> userStream = stream.process(new ProcessFunction<Event, String>() {
            @Override
            public void processElement(Event value, ProcessFunction<Event, String>.Context ctx,
                                       Collector<String> out) throws Exception {
                // 抽取Event的user到主输出流
                out.collect(value.user);
                // 侧输出流收集event的url
                ctx.output(outputTag, "side-output: " + value.url);
            }
        });

        userStream.print("user: ");
        // 获取侧输出流，和之前窗口API中获取侧输出流 方式一样
        DataStream<String> urlStream = userStream.getSideOutput(outputTag);
        urlStream.print("url :");

        env.execute();
    }
}
