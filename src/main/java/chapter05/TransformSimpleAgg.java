package chapter05;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformSimpleAgg {
    public static void main(String[] args) throws  Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.addSource(new ClickSource());
        stream.keyBy(
                new KeySelector<Event, Object>() {
                    @Override
                    public Object getKey(Event event) throws Exception {

                        return event.user;
                    }
                }
        ).maxBy("timestamp").print();

        env.execute();


    }
}
