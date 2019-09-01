import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

object FlinkKafkaSourceDemo {

  def main(args: Array[String]): Unit = {

    //  创建flink流式处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment


    import org.apache.flink.api.scala._
    //  添加kafka消费者

    val topic = "test_r1p2"
    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "hadoop128:9092")
    prop.setProperty("group.id", "con0901")

    val myCousumer = new FlinkKafkaConsumer011[String](topic, new SimpleStringSchema(), prop)
    val text = env.addSource(myCousumer)
    // 打印数据
    text.print()

    env.execute("FlinkKafkaSourceDemo")


  }

}
