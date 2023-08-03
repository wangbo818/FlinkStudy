package chapter08;

import chapter05.ClickSource;
import chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class UnionTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        SingleOutputStreamOperator<Event> stream1 = env.socketTextStream("localhost", 7777).map(
                data -> {
                    String[] field = data.split(",");
                    return new Event(field[0].trim(), field[1].trim(), Long.valueOf(field[2]));
                }
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event event, long l) {
                        return event.timestamp;
                    }
                }));
        stream1.print("stream1");


        SingleOutputStreamOperator<Event> stream2 = env.socketTextStream("localhost", 8888).map(
                data -> {
                    String[] field = data.split(",");
                    return new Event(field[0].trim(), field[1].trim(), Long.valueOf(field[2]));
                }
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event event, long l) {
                        return event.timestamp;
                    }
                }));
        stream2.print("stream2");


        //合并两条流
        stream1.union(stream2)
                .process(new ProcessFunction<Event, String>() {
                    @Override
                    public void processElement(Event event, ProcessFunction<Event, String>.Context context, Collector<String> collector) throws Exception {
                        collector.collect("水位线是： " + context.timerService().currentWatermark());
                    }
                }).print();

        env.execute();


    }
}
