package cn.itcast.hello;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @author zengwang
 * @create 2022-04-05 23:23
 * @desc: 演示Flink-DataStream-API 实现WordCount
 * 注意：再Flink1.12中DataStream即支持流处理也支持批处理，如何区分？
 */
public class WordCount4 {
    public static void main(String[] args) throws Exception {
        // 0.env 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // env.setRuntimeMode(RuntimeExecutionMode.BATCH); //使用DataStream实现批处理
        // 批处理 和流处理跑的结果不一样哈
        // env.setRuntimeMode(RuntimeExecutionMode.STREAMING); //使用DataStream实现流处理
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC); //使用DataStream根据数据源，自行选择使用流还是批

        // 1.source 加载数据
        DataStreamSource<String> lines = env.fromElements("itcast hadoop spark",
                "itcast hadoop spark", "itcast hadoop", "itcast");

        // 2.transformation
        // 切割，先写匿名内部类，后面再写lambda 表达式
        /*DataStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] arr = value.split(" ");
                for (String word : arr) {
                    out.collect(word);
                }
            }
        });*/
        // lambda 函数，（参数列表） -> 方法体(一行干不完，还可以用{}包起来)
        DataStream<String> words = lines.flatMap(
                (String value, Collector<String> out) -> {Arrays.stream(value.split(" ")).forEach(out::collect);}
        ).returns(Types.STRING);
        // 记为1
        /*DataStream<Tuple2<String, Integer>> wordToOne = words.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        });*/
        DataStream<Tuple2<String, Integer>> wordToOne = words.map((String value) -> Tuple2.of(value, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT));
        // 分组: 注意DataSet API中，分组是 groupBy；DataStream API中，分组是 keyBy
        /*
        @FunctionalInterface
        public interface KeySelector<IN, KEY> extends Function, Serializable {
            KEY getKey(IN value) throws Exception;
        }
         */
        // 我们分组的KEY的数据类型是String，所以KEY泛型也是String
        // 使用匿名内部类实现 取f0时报错，改为匿名函数
        KeyedStream<Tuple2<String, Integer>, String> grouped = wordToOne.keyBy(t -> t.f0);

        //聚合, 索引为1 的列
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = grouped.sum(1);

        // sink:输出 打印的前面数字，可以理解为线程编号
        result.print();
        // Stream env必须调用execute才会执行程序
        env.execute();
    }
}
