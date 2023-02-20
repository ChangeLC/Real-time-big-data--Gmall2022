package utill

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

// 读取Kafka工具类
object MyKafkaUtil {
  //读取配置文件信息
  private val properties = MyPropertiesUtil.load("config.properties")
  private val broker_list: AnyRef = properties.get("kafka.broker.list")

  // kafka 消费者配置
  var kafkaParam = collection.mutable.Map(
    "bootstrap.servers" -> broker_list,//用于初始化链接到集群的地址
    "key.deserializer" -> classOf[StringDeserializer],  //序列化方式
    "value.deserializer" -> classOf[StringDeserializer],
    //用于标识这个消费者属于哪个消费团体
    "group.id" -> "gmall2022_group",
    //latest 自动重置偏移量为最新的偏移量
    "auto.offset.reset" -> "latest",
    //如果是 true，则这个消费者的偏移量会在后台自动提交,但是 kafka 宕机容易丢失数据
    //如果是 false，会需要手动维护 kafka 偏移量
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  // 使用默认的消费者组消费数据
  // 返回值是一个离散化流，【K - V】，需要的数据就是V
  def getKafkaStream01(topic: String,ssc:StreamingContext ): InputDStream[ConsumerRecord[String,String]]={
    val dStream = KafkaUtils.createDirectStream[String,String](
      ssc,
      //位置策略，就是spark读取kafka文件的节点选择，这里选择的是机架距离
      LocationStrategies.PreferConsistent,
      //消费策略，指定消费一些主题的数据 ;kafkaParams:kafka配置信息
      ConsumerStrategies.Subscribe[String,String](Array(topic), kafkaParam )
    )
    dStream
  }

  // 指定消费者组消费数据
  def getKafkaStream02(topic: String,ssc:StreamingContext,groupId:String ): InputDStream[ConsumerRecord[String,String]]={
    kafkaParam("group.id")=groupId
    val dStream = KafkaUtils.createDirectStream[String,String](
      ssc,
      //位置策略，就是spark读取kafka文件的节点选择，这里选择的是机架距离
      LocationStrategies.PreferConsistent,
      //消费策略，指定消费一些主题的数据 ;kafkaParams:kafka配置信息
      ConsumerStrategies.Subscribe[String,String](Array(topic), kafkaParam )
    )
    dStream
  }

  // 从指定的偏移量读取数据
  def getKafkaStream03(topic: String, ssc:StreamingContext, offsets:Map[TopicPartition,Long], groupId:String ): InputDStream[ConsumerRecord[String,String]]={
    kafkaParam("group.id")=groupId
    val dStream = KafkaUtils.createDirectStream[String,String](
      ssc,
      //位置策略，就是spark读取kafka文件的节点选择，这里选择的是机架距离
      LocationStrategies.PreferConsistent,
      //消费策略，指定消费一些主题的数据 ;kafkaParams:kafka配置信息
      ConsumerStrategies.Subscribe[String,String](Array(topic), kafkaParam,offsets )
    )
    dStream
  }
}
