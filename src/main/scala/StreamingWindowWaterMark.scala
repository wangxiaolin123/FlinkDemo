
import java.text.SimpleDateFormat

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


object StreamingWindowWaterMark {

  def main(args: Array[String]): Unit = {

    val port = 9001

    //  搭建流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //设置使用 eventtime，默认是使用 processtime
    env.setStreamTimeCharacteristic( TimeCharacteristic.EventTime)
    // 设置并行度
    env.setParallelism(1)

    // 设置Source
    val text = env.socketTextStream("hadoop130", port, '\n')

    import org.apache.flink.api.scala._
    //  解析数据
    val inputMap: DataStream[(String, Long)] = text.map(line => {
      val arr: Array[String] = line.split(',')
      //  key,时间戳
      (arr(0), arr(1).toLong)
    })

    //  抽取timestamp，生成watermark
    val waterMarkStream = inputMap.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String, Long)] {

      var currentMaxTimestamp = 0L

      //  设置最大乱序时间
      val maxOutOfOrderness = 10000L

      //  日期格式化
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

      /**
        * 定义生成的watermark，默认每100毫秒调用一次
        *
        * @return
        */
      override def getCurrentWatermark: Watermark = {

        new Watermark(currentMaxTimestamp - maxOutOfOrderness)
      }

      // 抽取事务时间戳
      override def extractTimestamp(t: (String, Long), l: Long): Long = {
        val timeStamp = t._2
        currentMaxTimestamp = Math.max(timeStamp, currentMaxTimestamp)
        println("key: " + t._1 + " ,eventTime: " + sdf.format(t._2) + " currentMaxTime: " + sdf.format(currentMaxTimestamp) + " watermark: " + sdf.format(getCurrentWatermark.getTimestamp))
        timeStamp
      }
    })

    //  聚合，统计
    val window: DataStream[String] = waterMarkStream.keyBy(0)
      .window(TumblingEventTimeWindows.of(Time.seconds(3)))
      .apply(new WindowFunction[(String, Long), String, Tuple, TimeWindow] {

        /**
          * 对window内数据进行排序，保证数据的顺序
          */
        override def apply(key: Tuple, window: TimeWindow, input: Iterable[(String, Long)], out: Collector[String]): Unit = {

          //  根据时间戳大小排序，升序排列
          val list = input.toList.sortWith(_._2 < _._2)
          val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
          val result = key.toString + ", 集合大小: " + list.size + ", 开始于: " + sdf.format(list(0)._2) + ",结束于: " + sdf.format(list(list.size - 1)._2) + ", windowStart: " + sdf.format(window.getStart) + ",windowEnd: " + sdf.format(window.getEnd());
          out.collect(result)
        }
      })

    window.print()

    env.execute("StreamingWindowWatemark")

  }

}
