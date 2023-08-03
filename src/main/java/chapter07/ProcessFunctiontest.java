package chapter07;

import chapter05.ClickSource;
import chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class ProcessFunctiontest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        })
                );
        stream.process(new ProcessFunction<Event, String>() {
            @Override
            public void processElement(Event event, ProcessFunction<Event, String>.Context context, Collector<String> collector) throws Exception {
                if(event.user.equals("Mary")){
                    collector.collect(event.user + "clicks" + event.url);
                }else if(event.user.equals("Bob")){
                    collector.collect(event.user);
                    collector.collect(event.user);
                }
                collector.collect(event.toString());
                System.out.println("timestamp is "+context.timestamp());
                System.out.println("当前 watermark is "+context.timerService().currentWatermark());

                System.out.println(getRuntimeContext().getIndexOfThisSubtask());

            }


        }).print("ProcessTest");


        env.execute();


    }
}
