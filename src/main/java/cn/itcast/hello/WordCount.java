package cn.itcast.hello;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.stringtemplate.v4.ST;

/**
 * @author zengwang
 * @create 2022-04-05 23:23
 * @desc: 演示Flink-DataSet-API 实现WordCount
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        // 0.env 创建环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 1.source 加载数据
        // java的可变参数，这里是4行文本作为输入
        DataSet<String> lines = env.fromElements("itcast hadoop spark",
                "itcast hadoop spark", "itcast hadoop", "itcast");

        // 2.transformation
        // 切割，先写匿名内部类，后面再写lambda 表达式
        /*
        @FunctionalInterface
        @Public
        public interface FlatMapFunction<T, O> extends Function, Serializable {
            void flatMap(T var1, Collector<O> var2) throws Exception;
        }
         */
        // 从最本质的角度来看，flatMap 我们传出一行，切割后，我们需要返会一个个word
        // 一进多出，出去的数据类型还是string！
        DataSet<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> out) throws Exception {
                String[] arr = s.split(" ");
                for (String word : arr) {
                    out.collect(word);
                }
            }
        });

        // 记为1
        DataSet<Tuple2<String, Integer>> wordToOne = words.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return Tuple2.of(s, 1);
            }
        });
        // 分组，按照序号去分
        UnsortedGrouping<Tuple2<String, Integer>> grouped = wordToOne.groupBy(0);
        //聚合, 索引为1 的列
        AggregateOperator<Tuple2<String, Integer>> result = grouped.sum(1);

        // sink:输出
        result.print();
    }
}
