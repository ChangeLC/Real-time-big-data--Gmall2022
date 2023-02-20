package ods

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utill.{MyKafkaSink, MyKafkaUtil, OffsetManagerUtil}

//从kafka读取数据，根据表名进行分流处理
object BaseDBCanalApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("BaseDBCanalApp").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val topic: String = "gmall2022_db_c"
    val groupId: String = "base_db_canal_group"

    // 从redis中获取kafka偏移量
    val kafkaOffsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic,groupId)
    var recoredDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (kafkaOffsetMap!=null && kafkaOffsetMap.size >0){
      //从指定的偏移量消费
      recoredDStream = MyKafkaUtil.getKafkaStream03(topic,ssc,kafkaOffsetMap,groupId)
    }else{
      //从最新的位置消费
      recoredDStream = MyKafkaUtil.getKafkaStream02(topic,ssc,groupId)

    }


    // 获取当前批次的偏移量信息
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] = recoredDStream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }


    // 对接收到的数据进行结构转换，ConsumerRecord[String,String(jsonStr)]==>jsonObj
    val jsonObjDStream: DStream[JSONObject] = offsetDStream.map {
      record => {
        //获取json格式的字符串
        val jsonStr: String = record.value()
        // 将json格式字符串转换为json对象
        val jsonObject: JSONObject = JSON.parseObject(jsonStr)
        jsonObject
      }
    }



    // 分流，根据不同的表名，将数据发送到不同的Kafka主题中去

    jsonObjDStream.foreachRDD{
      rdd=>{
        rdd.foreach{
          jsonObj=>{
            //获取更新的表名
            val tableName: String = jsonObj.getString("table")
            //获取当前对表数据的更新
            val dataArr: JSONArray = jsonObj.getJSONArray("data")
            val opType: String = jsonObj.getString("type")
            //拼接发送的主题
            var sendTopic = "ods_" + tableName
            import scala.collection.JavaConverters._
            if("INSERT".equals(opType)){
                for (data <- dataArr.asScala) {
                    val msg: String = data.toString
                    //向 kafka 发送消息
                    MyKafkaSink.send(sendTopic,msg)
                }
            }
          }
        }
        //修改 Redis 中 Kafka 的偏移量
        OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}