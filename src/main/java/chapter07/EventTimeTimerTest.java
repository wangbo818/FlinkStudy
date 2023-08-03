package chapter07;

import chapter05.ClickSource;
import chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

public class EventTimeTimerTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new CustomSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));

        stream.keyBy(data -> data.user)
                .process(new KeyedProcessFunction<String, Event, String>() {
                    @Override
                    public void processElement(Event event, KeyedProcessFunction<String, Event, String>.Context context, Collector<String> collector) throws Exception {
                        Long currTs = context.timestamp();

                        collector.collect(context.getCurrentKey() + "  数据到达，时间戳：" + new Timestamp(currTs) +
                                "当前watermark is " + context.timerService().currentWatermark()
                        );

                        //注册一个10秒后的定时器
                        context.timerService().registerEventTimeTimer(currTs + 10 * 1000l);

                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

                        out.collect(ctx.getCurrentKey() + " 定时器触发，触发时间" + new Timestamp(timestamp)
                        +"  waterMark is " + ctx.timerService().currentWatermark()
                        );

                    }
                }).print();


        env.execute();

    }

    //自定义测试数据源
    public  static class CustomSource implements SourceFunction<Event>{

        @Override
        public void run(SourceContext<Event> sourceContext) throws Exception {
            sourceContext.collect(new Event("Mary","./home",1000L));
            Thread.sleep(5000L);

            sourceContext.collect(new Event("Alice","./home",11000L));
            Thread.sleep(5000L);

            sourceContext.collect(new Event("Bob","./home",12001L));
            Thread.sleep(5000L);

        }

        @Override
        public void cancel() {

        }
    }
}
