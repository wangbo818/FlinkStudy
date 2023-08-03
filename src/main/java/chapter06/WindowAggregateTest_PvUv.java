package chapter06;


import chapter05.ClickSource;
import chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;


import java.time.Duration;
import java.util.HashSet;

// 统计pv和nv，两者相除得到 pv所有次数， uv用户的访问次数
public class WindowAggregateTest_PvUv {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource()).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event event, long l) {
                        return event.timestamp;
                    }
                }));

        stream.print("Data");
        //所有数据放在一起 统计pv 和nv
        stream.keyBy(data -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new AvgPv()).print();
        env.execute();
    }
    //自定义一个 aggregateFunction 用Long保存pv个数，用HashSet做uv去重

    public static class AvgPv implements AggregateFunction<Event, Tuple2<Long, HashSet<String>>, Double> {


        @Override
        public Tuple2<Long, HashSet<String>> createAccumulator() {
            return Tuple2.of(0l, new HashSet<>());
        }

        @Override
        public Tuple2<Long, HashSet<String>> add(Event event, Tuple2<Long, HashSet<String>> longHashSetTuple2) {
            //每来一条 ， pv个数+1， 将user放入HashSet中

            longHashSetTuple2.f1.add(event.user);
            return Tuple2.of(longHashSetTuple2.f0 + 1, longHashSetTuple2.f1);


        }

        @Override
        public Double getResult(Tuple2<Long, HashSet<String>> longHashSetTuple2) {
            //窗口触发时， 输出pv和uv的比值


            return (double) longHashSetTuple2.f0 / longHashSetTuple2.f1.size();
        }

        @Override
        public Tuple2<Long, HashSet<String>> merge(Tuple2<Long, HashSet<String>> longHashSetTuple2, Tuple2<Long, HashSet<String>> acc1) {
            return null;
        }
    }
}
