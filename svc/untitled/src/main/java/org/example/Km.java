package org.example;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Km extends KeyedProcessFunction<String, Long, String> {

    private transient ValueState<Long> count;

    @Override
    public void open(OpenContext ctx)  throws Exception {
        count = getRuntimeContext().getState(
                new ValueStateDescriptor<>("event-count", Long.class)
        );
    }

    @Override
    public void processElement(Long value,
                               Context ctx,
                               Collector<String> out) throws Exception {


        Long currentCount = count.value();
        if (currentCount == null) {
            System.out.println("set up timer");
            // watermark required
            //ctx.timerService().registerEventTimeTimer(5000);

            long now = ctx.timerService().currentProcessingTime();
            ctx.timerService().registerProcessingTimeTimer(now+5000);
            currentCount = 0L;
        }
        currentCount++;
        count.update(currentCount);
        out.collect("Key: " + ctx.getCurrentKey() + ", Count: " + currentCount);
    }

    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext ctx,
            Collector<String> out) throws Exception {

        System.out.println("END");
        out.collect(
                "Compte " + ctx.getCurrentKey()
                        + " : aucune transaction depuis 10 secondes"
        );
    }

    @Override
    public boolean useInterruptibleTimers(ReadableConfig config) {
        return super.useInterruptibleTimers(config);
    }
}
