package chapter05;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Calendar;
import java.util.Random;

public class ClickSource implements ParallelSourceFunction<Event> {

    //标志位
    private Boolean running =true;

    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {

        Random random = new Random();

        //定义随机数据范围
        String[] users = {"Mary", "Alice", "Bob", "Cary"};
        String[] urls = {"./home", "./cart", "./fav", "./prod?id=1",
                "./prod?id=2"};
        while (running){
            String user = users[random.nextInt(users.length)];
            String url = urls[random.nextInt(urls.length)];
            long timeInMillis = Calendar.getInstance().getTimeInMillis();
            sourceContext.collect(new Event(user,url,timeInMillis));

            Thread.sleep(1000L);
        }



    }

    @Override
    public void cancel() {
        running = false;

    }
}
