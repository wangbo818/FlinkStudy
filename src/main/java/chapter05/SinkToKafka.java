package chapter05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

public class SinkToKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Event> dataStream = env.addSource(new ClickSource());

        SingleOutputStreamOperator<String> map = dataStream.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event event) throws Exception {
                return event.user + "-" +event.timestamp;
            }
        });
        map.print();
        map.addSink(new FlinkKafkaProducer<String>(
                "localhost:19092","mytest",new SimpleStringSchema()

        ));

        env.execute();


    }
}
