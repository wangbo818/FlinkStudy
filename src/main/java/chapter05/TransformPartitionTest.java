package chapter05;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformPartitionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Event> dataStream = env.addSource(new ClickSource());

        //1.随机分区
//        dataStream.shuffle().print().setParallelism(4);

        //2.轮询分区
//        dataStream.rebalance().print().setParallelism(4);



        //广播分区
//        dataStream.broadcast().print().setParallelism(4);

        //全局分区
        dataStream.global().print().setParallelism(4);

        //自定义分区





        env.execute();
    }
}
