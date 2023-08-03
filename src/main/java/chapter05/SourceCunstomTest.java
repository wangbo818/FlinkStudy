package chapter05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Random;

public class SourceCunstomTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        DataStreamSource<Event> eventDataStreamSource = env.addSource(new ClickSource());

        DataStreamSource<Integer> integerDataStreamSource = env.addSource(new ParallelCustomSource()).setParallelism(3);

        integerDataStreamSource.print();
        env.execute();
    }


    public static class ParallelCustomSource implements ParallelSourceFunction<Integer>{
        private  Boolean running = true;
        Random random = new Random();

        @Override
        public void run(SourceContext<Integer> sourceContext) throws Exception {
            while (running){
                sourceContext.collect(random.nextInt());
            }
        }

        @Override
        public void cancel() {
            running  = false;
        }
    }

}


