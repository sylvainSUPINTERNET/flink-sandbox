package org.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;



//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {

    public record Transaction(Long accountId, Double amount) { }

    public static void main(String[] args) throws Exception {
        //TIP Press <shortcut actionId="ShowIntentionActions"/> with your caret at the highlighted text
        // to see how IntelliJ IDEA suggests fixing it.


        var env = StreamExecutionEnvironment
                .createRemoteEnvironment(
                        "172.19.235.201",
                        8081,
                        "target/untitled-1.0-SNAPSHOT.jar"
                );

        env.setParallelism(4);

        DataGeneratorSource<Long> source =
                new DataGeneratorSource<>(
                        index -> index,
                        Long.MAX_VALUE,
                        RateLimiterStrategy.perSecond(1),
                        Types.LONG
                );

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Data Generator Source")
                .print();

        env.execute("Data Generator Job");

        /*
        var sourceA = env.fromSequence(1, 1_000);
        var sourceB = env.fromSequence(1001, 2000);
        sourceA.union(sourceB)
                .map( v -> v*2)
                .filter(v -> v % 3 == 0)
                .print();
         */

        /*
        var source = env.fromSequence(1, 10);

        source.map(i -> i%10)
                .map( i -> {
                    return new Transaction(i, i * 2.0);
                })
                .keyBy(Transaction::accountId)
                .reduce((a,b) -> {
                    return new Transaction(a.accountId(), a.amount() + b.amount());
                })
                .print();
        */


        /*
        env.fromSequence(1, 1_000_000)
                .name("Numbers Source")
                .map(new DoubleValue())
                .name("Double Values")
                .keyBy(value -> value % 10)
                .filter(new IsDivisibleByThree())
                .name("Divisible By Three")
                .print();

        env.execute("Explore Flink");
        */


        /*
        env.fromSequence(1, 1_000_000)
                .map(new DoubleValue())
                .keyBy(value -> value % 10)
                .filter(new IsDivisibleByThree())
                .print();

        env.execute("Remote Flink Job");
        */
    }
}
