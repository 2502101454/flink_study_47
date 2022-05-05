package com.atguigu.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author zengwang
 * @create 2022-05-02 17:08
 * @desc: 流式处理 有界流(批数据)
 */
public class BoundedStreamWordCount {
    public static void main(String[] args) throws Exception {
        // 1.创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.读取文件
        DataStreamSource<String> lineDataStreamSource = env.readTextFile("input/words.txt");
        // 3.转换计算
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOneTuple = lineDataStreamSource.flatMap(
                (String line, Collector<Tuple2<String, Long>> out) -> {
                    String[] words = line.split(" ");
                    for (String word : words) {
                        out.collect(Tuple2.of(word, 1L));
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.LONG));
        // 4.分组
        KeyedStream<Tuple2<String, Long>, String> wordAndOneKeyedStream = wordAndOneTuple.keyBy(data -> data.f0);
        // 5.求和, 取索引1的列
        SingleOutputStreamOperator<Tuple2<String, Long>> result = wordAndOneKeyedStream.sum(1);
        // 6.打印
        result.print();

        // 7.上面只是定义了处理过程，对于流式处理程序而言，需要启动执行
        env.execute();
    }
}
