import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper

object FlinkKafkaSinkDemo {

  def main(args: Array[String]): Unit = {

    //  搭建Flink环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment


    //  想要支持仅一次语句，需要开启checkpoint
    env.enableCheckpointing(5000)
    //  使用socket作为源
    val text = env.socketTextStream("hadoop128", 9999, '\n')

    // 假装对text做了处理

    //  创建kafkasink
    val topic2 = "fks_r1p2"
    val prop2 = new Properties()
    prop2.setProperty("bootstrap.servers","hadoop128:9092")

    // FlinkKafkaProducer011默认 仅能提供至少一次语义
    //val myProducer = new FlinkKafkaProducer011[String](topic2, new SimpleStringSchema(), prop2)

    //  要想实现仅且一次的语义，有两种方法
    //  方法1 调整生产者的事务超时时间
    prop2.setProperty("transaction.timeout.ms",60000*15+"")

    //  可以提供仅一次语义
    val myProducer = new FlinkKafkaProducer011[String](topic2,new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema()), prop2, FlinkKafkaProducer011.Semantic.EXACTLY_ONCE)

    //  方法2 修改Kafka集群配置 在server.properties中增加transaction.max.timeout.ms，值为900000

    text.addSink(myProducer)




    env.execute("FlinkKafkaSinkDemo")

  }

}
