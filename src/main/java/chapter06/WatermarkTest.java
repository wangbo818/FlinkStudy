package chapter06;

import chapter05.ClickSource;
import chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class WatermarkTest {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Event> dataStream = env.addSource(new ClickSource());

        // 乱序流的watermark
        dataStream.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(

                new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event o, long l) {
                        return o.timestamp;
                    }


                }

        ));

        env.execute();
    }
}
