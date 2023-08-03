package chapter05;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TransformFlapMapTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        SingleOutputStreamOperator<String> flatMap2 = stream.flatMap(
                (FlatMapFunction<Event, String>) (event, collector) -> {
            collector.collect(event.user);
            collector.collect(event.timestamp.toString());

        }).returns(new TypeHint<String>() {
        });
        flatMap2.print();

        env.execute();

    }
}
