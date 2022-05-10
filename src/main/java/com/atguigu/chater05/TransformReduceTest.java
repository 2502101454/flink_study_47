package com.atguigu.chater05;

import io.netty.handler.codec.http2.Http2Exception;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zengwang
 * @create 2022-05-10 8:47 下午
 * @desc:
 */
public class TransformReduceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Bob", "./home", 3100L),
                new Event("Alice", "./profile", 3200L),
                new Event("Bob", "./prod?id=1", 3300L)
        );

        SingleOutputStreamOperator<Tuple2<String, Long>> userOne = stream.map(data -> Tuple2.of(data.user, 1L))
                .returns(new TypeHint<Tuple2<String, Long>>() {});
        // 1.统计每个用户的访问频次
        SingleOutputStreamOperator<Tuple2<String, Long>> clicksByUser = userOne.keyBy(data -> data.f0)
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> t1, Tuple2<String, Long> t2) throws Exception {
                        return Tuple2.of(t1.f0, t1.f1 + t2.f1);
                    }
                });

        // 2.求全部用户中，访问频次最大的用户
        // 生产环境，全局聚合慎用
        SingleOutputStreamOperator<Tuple2<String, Long>> maxUser = clicksByUser.keyBy(data -> "key")
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> t1, Tuple2<String, Long> t2) throws Exception {
                        return t1.f1 > t2.f1 ? t1 : t2;
                    }
                });

        maxUser.print();

        env.execute();
    }
}
