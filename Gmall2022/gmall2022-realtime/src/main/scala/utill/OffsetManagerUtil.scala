package utill

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

import java.util

// 维护偏移量工具类
object OffsetManagerUtil {
  def saveOffset(topic: String, groupId: String, offsetRanges: Array[OffsetRange]): Unit = {
    // 拼接操作redis的key  offset:topic:groupId
    var offsetKey = "offset" + topic + ":" +groupId

    //定义java的map集合，用于存放每个分区对应的偏移量
    val offsetMap: util.HashMap[String, String] = new util.HashMap[String, String]()

    // 对offsetRanger进行遍历，将数据封装到offsetMap中
    for(offsetRang <- offsetRanges){
      val partition: Int = offsetRang.partition
      val fromOffset: Long = offsetRang.fromOffset
      val untilOffset: Long = offsetRang.untilOffset
      offsetMap.put(partition.toString,untilOffset.toString)
      println("保存分区"+partition+":"+fromOffset+"--->"+untilOffset)
    }
    // 获取客户端
    val jedis: Jedis = MyRedisUtil.getJedisClient()

    jedis.hmset(offsetKey,offsetMap) //(offset topic:groupId，{分区1:000 ；分区2:000；分区3:000})

    jedis.close()
  }


  // 从redis中获取偏移量  type:hash  key:  offset:topic:groupId    field:partition  value: 偏移量
  def getOffset(topic:String,groupId:String):Map[TopicPartition,Long]={
    // 获取客户端连接
    val jedis:Jedis = MyRedisUtil.getJedisClient()

    // 拼接操作redis的key  offset:topic:groupId
    var offsetKey = "offset" + topic + ":" +groupId

    // 获取当前消费者组消费的主题   对应的分区以及偏移量
    val offsetMap: util.Map[String, String] = jedis.hgetAll(offsetKey)

    // 关闭客户端
    jedis.close()

    // 将java的map转换成scala的map
    import scala.collection.JavaConverters._

    val oMap: Map[TopicPartition, Long] = offsetMap.asScala.map {
      case (partition, offset) => {
        // Map[TopicPartition,Long]
        (new TopicPartition(topic, partition.toInt), offset.toLong)
      }
    }.toMap

    oMap

  }
}
