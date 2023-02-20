package dim


import bean.ProvinceInfo
import com.alibaba.fastjson.JSON
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import utill.{MyKafkaUtil, OffsetManagerUtil}

/**
 * Author: Felix
 * Desc: 从 Kafka 中读取省份维度数据，写入到 Hbase 中
 */
object ProvinceInfoApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("ProvinceInfoApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ods_base_province"
    val groupId = "province_info_group"

    //从 Redis 中读取 Kafka 偏移量
    val kafkaOffsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic,groupId)
    var recordDstream: InputDStream[ConsumerRecord[String, String]] = null
    if(kafkaOffsetMap!=null&&kafkaOffsetMap.size>0){
          //Redis 中有偏移量 根据 Redis 中保存的偏移量读取
            recordDstream = MyKafkaUtil.getKafkaStream03(topic, ssc,kafkaOffsetMap,groupId)
    }else{
          // Redis 中没有保存偏移量 Kafka 默认从最新读取
          recordDstream = MyKafkaUtil.getKafkaStream02(topic, ssc,groupId)
    }

    //得到本批次中处理数据的分区对应的偏移量起始及结束位置
    // 注意：这里我们从 Kafka 中读取数据之后，直接就获取了偏移量的位置，因为 KafkaRDD 可以转 换为 HasOffsetRanges，会自动记录位置
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] =
      recordDstream.transform {
        rdd => {offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          rdd
        }
      }

    //写入到 Hbase 中
    offsetDStream.foreachRDD{
      rdd=>{
                val provinceInfoRDD: RDD[ProvinceInfo] = rdd.map {
                  record => {
                    //得到从 kafka 中读取的 jsonString
                    val jsonString: String = record.value()
                    //转换为 ProvinceInfo
                    val provinceInfo: ProvinceInfo = JSON.parseObject(jsonString, classOf[ProvinceInfo])
                    provinceInfo
                  }
                }


                //保存到 hbase
                import org.apache.phoenix.spark._
                provinceInfoRDD.saveToPhoenix(
                  "gmall2022_province_info",
                  Seq("ID","NAME","AREA_CODE","ISO_CODE"),
                  new Configuration,
                  Some("hadoop102,hadoop103,hadoop104:2181")
                )

                //提交偏移量
                OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
      }
    }



    ssc.start()
    ssc.awaitTermination()

  }

}