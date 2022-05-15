package com.atguigu.chater05;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zengwang
 * @create 2022-05-15 10:24 上午
 * @desc:
 */
public class SinkToMysql {
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

        stream.addSink(JdbcSink.sink(
                "INSERT INTO clicks (user, url) values (?, ?)",
                // 第一个参数是当前的sql对应的statement，第二个参数就是过来的每条数据
                (statement, event) -> {
                    statement.setString(1, event.user);
                    statement.setString(2, event.url);
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3306/wz_test?useSSL=false&serverTimezone=UTC")
                        // 对于 MySQL 5.7，用"com.mysql.jdbc.Driver"
                        // 我这里运行异常了，大概原因：
                        // 我本地mac  mysql版本是8.0.28，jar包啥都得换，先不管了
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("12345678")
                        .build()
        ));

        env.execute();
    }
}
