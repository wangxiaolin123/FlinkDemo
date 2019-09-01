import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**** Watermark 案例 ** Created by xuwei.tech. */


public class StreamingWindowWatermark {
    public static void main(String[] args) throws Exception {
        //定义 socket 的端口号
        int port = 9001;
        // 获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置使用 eventtime，默认是使用 processtime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置并行度为 1,默认并行度是当前机器的 cpu 数量
        env.setParallelism(1);
        //连接 socket 获取输入的数据
        DataStream<String> text = env.socketTextStream("hadoop130", port, "\n");
        //解析输入的数据
        DataStream<Tuple2<String, Long>> inputMap = text.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override public Tuple2<String, Long> map(String value)
                    throws Exception {
                String[] arr = value.split(",");
                return new Tuple2<>(arr[0], Long.parseLong(arr[1]));
            }
        });

        //抽取 timestamp 和生成 watermark
        DataStream<Tuple2<String, Long>> waterMarkStream = inputMap.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, Long>>() {
            Long currentMaxTimestamp = 0L;
            // 最大允许的乱序时间是 10s
            final Long maxOutOfOrderness = 10000L;
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            /***
             * 定义生成 watermark 的逻辑 * 默认 100ms 被调用一次
             * */
            @Nullable @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
            }
            //定义如何提取 timestamp
            @Override
            public long extractTimestamp(Tuple2<String, Long> element, long previousElementTimestamp) {
                long timestamp = element.f1;
                currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                //  显示 key 以及key的event时间戳，当前最大时间戳，以及watermark的时间戳
                System.out.println("key:"+element.f0+",eventtime:["+element.f1+"|"+sdf.format(element.f1)+"], currentMaxTimestamp:["+currentMaxTimestamp+"|"+ sdf.format(currentMaxTimestamp)+"],watermark:["+getCurrentWatermark().getTimestamp()+"|" +sdf.format(getCurrentWatermark().getTimestamp())+"]");
               //   返回的是event时间戳
                return timestamp;
            }
        });


        //分组，聚合
        DataStream<String> window = waterMarkStream.keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))// 按 照 消 息 的 EventTime 分配窗口，和调用 TimeWindow 效果一样
                .apply(new WindowFunction<Tuple2<String, Long>, String, Tuple, TimeWindow>() {
                    /*** 对 window 内的数据进行排序，保证数据的顺序
                     * * @param tuple * @param window * @param input * @param out * @throws Exception
                     * */
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<String> out) throws Exception {
                        String key = tuple.toString();
                        List<Long> arrarList = new ArrayList<Long>();
                        Iterator<Tuple2<String, Long>> it = input.iterator();
                        while (it.hasNext()) {
                            Tuple2<String, Long> next = it.next();
                            arrarList.add(next.f1);

                        }
                        Collections.sort(arrarList);
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                        String result = key + ", arrList: " + arrarList.size() + ", start: " + sdf.format(arrarList.get(0)) + ",end: " + sdf.format(arrarList.get(arrarList.size() - 1)) + ", windowStart: " + sdf.format(window.getStart()) + ",windowEnd: " + sdf.format(window.getEnd());
                        out.collect(result);
                    }
                });
//测试-把结果打印到控制台即可
        window.print();
//注意：因为 flink 是懒加载的，所以必须调用 execute 方法，上面的代码才会执行
        env.execute("eventtime-watermark"); }
}