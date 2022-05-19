package com.atguigu.chapter05;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * @author zengwang
 * @create 2022-05-13 10:29 下午
 * @desc:
 */
public class TransformPartitionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

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
        // fromElements只支持并行度1

        // 1. 随机分区
        // stream.shuffle().print().setParallelism(4);

        // 2. 轮询分区，发牌
        // stream.rebalance().print().setParallelism(4);

        // 3. 重缩放分区
        // 为了获取运行时上下文，使用富函数版本，并行数据源
        env.addSource(new RichParallelSourceFunction<Object>() {
            @Override
            public void run(SourceContext<Object> sourceContext) throws Exception {
                for (int i = 1; i<= 8; i++) {
                    // 下面会设置并行度为2
                    // 0号并行子任务一定获取的是偶数，1号则是奇数
                    if (i % 2 == getRuntimeContext().getIndexOfThisSubtask()) {
                        sourceContext.collect(i);
                    }

                }
            }

            @Override
            public void cancel() {

            }
        }).setParallelism(2)
                .rescale();
                //.global()
                //.print().setParallelism(4);

        // 4. 广播
        // stream.broadcast().print().setParallelism(4);

        // 5. 全局分区 上面global

        // 6. 自定义分，实现奇数发往1号分区，偶数发往0号分区
        env.fromElements(1, 2, 3, 4, 5, 6, 7, 8)
                        .partitionCustom(new Partitioner<Integer>() {
                            // 从KeySelector中获取key后，传入partition 函数，
                            // 然后返回分区编号, numPartitions是下游算子的并行度个数
                            @Override
                            public int partition(Integer key, int numPartitions) {
                                System.out.println("numPartitions: " + numPartitions);
                                return key % 2;
                            }
                        }, new KeySelector<Integer, Integer>() {
                            @Override
                            public Integer getKey(Integer value) throws Exception {
                                return value;
                            }
                        }).print().setParallelism(3);
        env.execute();
    }
}
