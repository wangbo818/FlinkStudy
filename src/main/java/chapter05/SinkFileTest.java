package chapter05;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.operators.StreamSink;

import java.util.concurrent.TimeUnit;

public class SinkFileTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Event> dataStream = env.addSource(new ClickSource());
        StreamingFileSink<String> streamingFileSink = StreamingFileSink.<String>forRowFormat(new Path("./output"), new SimpleStringEncoder<>("utf-8"))
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        .withMaxPartSize(1024 * 1024 * 1024)
                        .withRolloverInterval(TimeUnit.MINUTES.toMillis(10))
                        .withInactivityInterval(TimeUnit.MINUTES.toMillis(1))
                        .build())
                .build();
        dataStream.map(data -> data.toString()).addSink(streamingFileSink);
        env.execute();

    }
}
