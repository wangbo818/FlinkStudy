package chapter05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformMapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> data = env.addSource(new ClickSource());
        //自定义mapFunction
        SingleOutputStreamOperator<String> map = data.map(new MyMapper());

        //使用匿名类实现mapfunction借口
        SingleOutputStreamOperator<Object> map1 = data.map(new MapFunction<Event, Object>() {
            @Override
            public Object map(Event event) throws Exception {
                return event.url;
            }
        });

        //传入Lambda表达式
        SingleOutputStreamOperator<String> map2 = data.map(
                datasd -> datasd.user

        );
        map2.print();
//        map.print();
//        map1.print();
        env.execute();


    }

    public static class MyMapper implements MapFunction<Event,String>{

        @Override
        public String map(Event event) throws Exception {
           return event.user;
        }
    }

}
