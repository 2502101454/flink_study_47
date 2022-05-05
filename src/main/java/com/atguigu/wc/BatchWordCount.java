package com.atguigu.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author zengwang
 * @create 2022-05-02 16:09
 * @desc: 使用DataSetAPI
 */
public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 2.从文件读取数据
        DataSource<String> lineDataSource = env.readTextFile("input/words.txt");
        // 3.将每行数据进行分词，转换成二元组类型
        // 函数式编程式 java8引入的新特性，对于Flink而言，lambda表达式在编译时会出现泛型擦除
        // 返回值类型，<String 代表输入, Tuple2<String, Long> 代表输出>
        FlatMapOperator<String, Tuple2<String, Long>> wordAndOneTuple = lineDataSource.flatMap(
                (String line, Collector<Tuple2<String, Long>> out) -> {
                    // 将一行文本进行分词
                    String[] words = line.split(" ");
                    // 将每个单词转成二元组输出
                    for (String word : words) {
                        out.collect(Tuple2.of(word, 1L));
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.LONG));
        // 4.按照word进行分组，Flink中没有groupByKey，这里指定索引分组，0代表word
        UnsortedGrouping<Tuple2<String, Long>> wordAndOneGroup = wordAndOneTuple.groupBy(0);
        // 5.分组内进行聚合统计，使用索引1位置
        AggregateOperator<Tuple2<String, Long>> sum = wordAndOneGroup.sum(1);
        sum.print();

    }
}
