package dim

import bean.BaseTrademark
import com.alibaba.fastjson.JSON
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utill.{MyKafkaUtil, OffsetManagerUtil}
/**
 * Author: Felix
 * Desc: 读取商品品牌维度数据到 Hbase
 */
object BaseTrademarkApp {
  def main(args: Array[String]): Unit = {


    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("BaseTrademarkApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ods_base_trademark";
    val groupId = "dim_base_trademark_group"


    //偏移量处理
    val offset: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)
    var inputDstream: InputDStream[ConsumerRecord[String, String]] = null
    // 判断如果从 redis 中读取当前最新偏移量 则用该偏移量加载 kafka 中的数据 否则直接用 kafka 读出默认最新的数据
    if (offset != null && offset.size > 0) {
        inputDstream = MyKafkaUtil.getKafkaStream03(topic, ssc, offset, groupId)
    } else {
        inputDstream = MyKafkaUtil.getKafkaStream02(topic, ssc, groupId)
    }


    //取得偏移量步长
    var offsetRanges: Array[OffsetRange] = null
    val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] =
      inputDstream.transform {
        rdd =>{
          offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          rdd
        }
      }


    // 封装数据
    val objectDstream: DStream[BaseTrademark] = inputGetOffsetDstream.map {
      record =>{
        val jsonStr: String = record.value()
        val obj: BaseTrademark = JSON.parseObject(jsonStr, classOf[BaseTrademark])
        obj
      }
    }


    import org.apache.phoenix.spark._
    //保存到 Hbase
    objectDstream.foreachRDD{rdd=>
      rdd.saveToPhoenix(
        "GMALL2022_BASE_TRADEMARK",
        Seq("ID", "TM_NAME" ),
        new Configuration,Some("hadoop102,hadoop103,hadoop104:2181"))
      OffsetManagerUtil.saveOffset(topic,groupId, offsetRanges)
    }
    ssc.start()
    ssc.awaitTermination()
  }
}