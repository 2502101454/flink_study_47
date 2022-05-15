package com.atguigu.chater05;

import com.google.common.eventbus.DeadEvent;
import jodd.datetime.TimeUtil;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

/**
 * @author zengwang
 * @create 2022-05-14 12:12 下午
 * @desc:
 */
public class SinkToFileTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Bob", "./home", 3100L),
                new Event("Alice", "./profile", 3200L),
                new Event("Bob", "./prod?id=1", 3300L),
                new Event("steven", "./home", 3300L),
                new Event("tomas", "./prod?id=3", 3400L)
        );

        StreamingFileSink<String> streamingFileSink = StreamingFileSink.<String>forRowFormat(
                new Path("./output/"),
                new SimpleStringEncoder<>("UTF-8")
        ).withRollingPolicy(
                // 指定滚动策略
                DefaultRollingPolicy.builder()
                        // 单位byte
                        .withMaxPartSize(1024 * 1024 * 1024)
                        // 15分钟一切分
                        .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                        // 最长10分钟没有数据来，则切分
                        .withInactivityInterval(TimeUnit.MINUTES.toMillis(10))
                        .build()
        ).build();

        stream.map(data -> data.toString())
                .addSink(streamingFileSink);

        env.execute();

    }
}
