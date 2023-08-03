package chapter08;

import jdk.internal.org.objectweb.asm.tree.analysis.Value;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
//import sun.tools.java.Type;

import java.time.Duration;

public class BillCheckExample {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        //来自app的支付日志数据
        SingleOutputStreamOperator<Tuple3<String, String, Long>> appStream = env.fromElements(

                Tuple3.of("order-1", "app", 1000l),
                Tuple3.of("order-2", "app", 2000l),
                Tuple3.of("order-3", "app", 3500l)

        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> stringStringLongTuple3, long l) {
                        return stringStringLongTuple3.f2;
                    }
                }));
        //来自第三方支付平台的日志
        SingleOutputStreamOperator<Tuple4<String, String, String, Long>> thirdpartStream = env.fromElements(

                Tuple4.of("order-1", "app", "success", 3000l),
                Tuple4.of("order-3", "app", "success", 4000l)

        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple4<String, String, String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple4<String, String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple4<String, String, String, Long> stringStringStringLongTuple4, long l) {
                        return stringStringStringLongTuple4.f3;
                    }
                }));

        //检测同一支付订单在两条流中是否匹配， 不匹配就报警
//        appStream.keyBy(data -> data.f0)
//                .connect(thirdpartStream.keyBy(data -> data.f0));

        appStream.connect(thirdpartStream).keyBy(new KeySelector<Tuple3<String, String, Long>, Object>() {
                    @Override
                    public Object getKey(Tuple3<String, String, Long> stringStringLongTuple3) throws Exception {
                        return stringStringLongTuple3.f0;
                    }
                }, new KeySelector<Tuple4<String, String, String, Long>, Object>() {
                    @Override
                    public Object getKey(Tuple4<String, String, String, Long> stringStringStringLongTuple4) throws Exception {
                        return stringStringStringLongTuple4.f0;
                    }
                }).process(new OrderMatchResult())
                .print();

        env.execute();


    }

    //自定义实现CoPorcessFunction
    public static class OrderMatchResult extends CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String> {
        //定义状态变量，，用来保存已经到达的事件
        private ValueState<Tuple3<String, String, Long>> appEventState;
        private ValueState<Tuple4<String, String, String, Long>> thirdPartyEventState;


        @Override
        public void processElement1(Tuple3<String, String, Long> stringStringLongTuple3, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.Context context, Collector<String> collector) throws Exception {
            //来的是 appEvent的 ，看对应的另一条流中 事件是否来过
            if (thirdPartyEventState.value() != null) {
                collector.collect("对账成功" + stringStringLongTuple3 + "   " + thirdPartyEventState.value());
                //clean状态
                thirdPartyEventState.clear();
            } else {
                //更新 状态
                appEventState.update(stringStringLongTuple3);
                // 注册一个5s的定时器，开始等待另一条流的事件
                context.timerService().registerEventTimeTimer(stringStringLongTuple3.f2 + 5000);

            }


        }

        @Override
        public void processElement2(Tuple4<String, String, String, Long> stringStringStringLongTuple4, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.Context context, Collector<String> collector) throws Exception {

            if (appEventState.value() != null) {
                collector.collect("对账成功： " + appEventState.value() + "  " + stringStringStringLongTuple4);
                appEventState.clear();
            } else {

                //更新状态
                thirdPartyEventState.update(stringStringStringLongTuple4);
                //注册定时器5s
                context.timerService().registerEventTimeTimer(stringStringStringLongTuple4.f3 + 5000l);
            }


        }

        @Override
        public void onTimer(long timestamp, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发， 判断状态，如果某个状态为null 就说明  另一个流的中的事件没来

            if (appEventState.value() != null) {
                out.collect("对账失败： " + appEventState.value() + " " + "第三方支付平台信息未到");
            }
            if (thirdPartyEventState.value() != null) {
                out.collect("对账失败： " + thirdPartyEventState.value() + " " + "app支付平台信息未到");
            }
            appEventState.clear();
            thirdPartyEventState.clear();

        }

        @Override
        public void open(Configuration parameters) throws Exception {
            appEventState = getRuntimeContext().getState(new ValueStateDescriptor<Tuple3<String, String, Long>>("app-event", Types.TUPLE(Types.STRING, Types.STRING, Types.LONG)));
            thirdPartyEventState = getRuntimeContext().getState(new ValueStateDescriptor<Tuple4<String, String, String, Long>>("third-event", Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.LONG)));
        }
    }
}
