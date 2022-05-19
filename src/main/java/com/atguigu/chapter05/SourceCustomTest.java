package com.atguigu.chapter05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Random;

/**
 * @author zengwang
 * @create 2022-05-05 23:22
 * @desc:
 */
public class SourceCustomTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        // 有了自定义的source function，调用addSource方法
        // DataStreamSource<Event> customStream = env.addSource(new ClickSource());
        DataStreamSource<Integer> customStream = env.addSource(new CustomSource()).setParallelism(2);
        // print算子，并行度取env的，为4
        customStream.print();
        env.execute();
    }

    public static class CustomSource implements ParallelSourceFunction<Integer> {

        private boolean running = true;
        private Random random = new Random();

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            while (running) {
                ctx.collect(random.nextInt());
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
