package chapter05;

import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformRichFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        DataStreamSource<Event> dataStream = env.addSource(new ClickSource());
        SingleOutputStreamOperator<Integer> map = dataStream.map(new MyRichFunction());
        map.setParallelism(4).print();
        env.execute();


    }
    public static class MyRichFunction extends RichMapFunction<Event,Integer> {

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            System.out.println("open生命周期" + getRuntimeContext().getIndexOfThisSubtask() + "号任务");
        }

        @Override
        public Integer map(Event event) throws Exception {
            return event.user.length();
        }

        @Override
        public void close() throws Exception {
            super.close();
            System.out.println("close生命周期" + getRuntimeContext().getIndexOfThisSubtask() + "号任务");
        }
    }
}
