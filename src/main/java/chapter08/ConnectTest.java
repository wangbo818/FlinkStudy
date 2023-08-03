package chapter08;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class ConnectTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> stream1 = env.fromElements(1, 2, 3);
        DataStreamSource<Long> stream2 = env.fromElements(4l, 5l, 6l);

        SingleOutputStreamOperator<String> map = stream1.connect(stream2)
                .map(new CoMapFunction<Integer, Long, String>() {
                    @Override
                    public String map1(Integer integer) throws Exception {
                        return "第一" + integer;
                    }

                    @Override
                    public String map2(Long aLong) throws Exception {
                        return "第二" + aLong;
                    }


                });


        env.execute();

    }



}
