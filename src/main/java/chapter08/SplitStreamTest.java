package chapter08;

import chapter05.ClickSource;
import chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.log4j.xml.SAXErrorHandler;

import java.time.Duration;

public class SplitStreamTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource()).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event event, long l) {
                        return event.timestamp;
                    }
                }));

        //定义输出标签
        OutputTag<Tuple3<String, String, Long>> maryTag = new OutputTag<Tuple3<String,String,Long>>("Mary"){};
        OutputTag<Tuple3<String, String, Long>> bobTag = new OutputTag<Tuple3<String,String,Long>>("Bob"){};


        SingleOutputStreamOperator<Event> processStream = stream.process(new ProcessFunction<Event, Event>() {
            @Override
            public void processElement(Event event, ProcessFunction<Event, Event>.Context context, Collector<Event> collector) throws Exception {

                if (event.user.equals("Mary")) {
                    context.output(maryTag, Tuple3.of(event.user, event.url, event.timestamp));
                } else if (event.user.equals("Bob")) {
                    context.output(bobTag, Tuple3.of(event.user, event.url, event.timestamp));
                } else {

                    collector.collect(event);
                }


            }
        });
        processStream.print("else");
        processStream.getSideOutput(maryTag).print("mary");
        processStream.getSideOutput(bobTag).print("Bob");



        env.execute();
    }
}
