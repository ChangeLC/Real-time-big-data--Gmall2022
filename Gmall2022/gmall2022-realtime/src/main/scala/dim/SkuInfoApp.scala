package dim

import bean.SkuInfo
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utill.{MyKafkaUtil, OffsetManagerUtil, PhoenixUtil}
/**
 * Author: Felix
 * Desc: 读取商品维度数据，并关联品牌、分类、Spu，保存到 Hbase
 */
object SkuInfoApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("SkuInfoApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ods_sku_info";
    val groupId = "dim_sku_info_group"

    ///////////////////// 偏移量处理///////////////////////////
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


    //结构转换
    val objectDstream: DStream[SkuInfo] = inputGetOffsetDstream.map {
      record =>{
        val jsonStr: String = record.value()
        val obj: SkuInfo = JSON.parseObject(jsonStr, classOf[SkuInfo])
        obj
      }
    }


    //商品和品牌、分类、Spu 先进行关联
    val skuInfoDstream: DStream[SkuInfo] = objectDstream.transform {
      rdd =>{
        if(rdd.count()>0){

          //tm_name
          val tmSql = "select id ,tm_name from gmall2022_base_trademark"
          val tmList: List[JSONObject] = PhoenixUtil.queryList(tmSql)
          val tmMap: Map[ String, JSONObject] = tmList.map(
            jsonObj => (jsonObj.getString("ID"), jsonObj)
          ).toMap


          //category3
          val category3Sql = "select id ,name from gmall2022_base_category3"
          //driver 周期性执行
          val category3List: List[JSONObject] = PhoenixUtil.queryList(category3Sql)
          val category3Map: Map[String, JSONObject] = category3List.map(
            jsonObj => (jsonObj.getString("ID"), jsonObj)
          ).toMap


          // spu
          val spuSql = "select id ,spu_name from gmall2022_spu_info" // spu
          val spuList: List[JSONObject] = PhoenixUtil.queryList(spuSql)
          val spuMap: Map[String, JSONObject] = spuList.map(
            jsonObj => (jsonObj.getString("ID"), jsonObj)
          ).toMap


          // 汇总到一个 list 广播这个 map
          val dimList = List[Map[String, JSONObject]](category3Map,tmMap,spuMap)
          val dimBC: Broadcast[List[Map[String, JSONObject]]] = ssc.sparkContext.broadcast(dimList)
          val skuInfoRDD: RDD[SkuInfo] = rdd.mapPartitions {
              skuInfoItr =>{

                  //ex
                  val dimList: List[Map[String, JSONObject]] = dimBC.value//接收 bc
                  val category3Map: Map[String, JSONObject] = dimList(0)
                  val tmMap: Map[String, JSONObject] = dimList(1)
                  val spuMap: Map[String, JSONObject] = dimList(2)


                  val skuInfoList: List[SkuInfo] = skuInfoItr.toList
                  for (skuInfo <- skuInfoList) {

                      val category3JsonObj: JSONObject = category3Map.getOrElse(skuInfo.category3_id , null) //从 map 中寻值
                      if (category3JsonObj != null) {
                            skuInfo.category3_name = category3JsonObj.getString("NAME")
                      }

                      val tmJsonObj: JSONObject = tmMap.getOrElse(skuInfo.tm_id , null)
                      //从 map 中寻值
                      if (tmJsonObj != null) {
                                 skuInfo.tm_name = tmJsonObj.getString("TM_NAME")
                      }


                      val spuJsonObj: JSONObject = spuMap.getOrElse(skuInfo.spu_id , null)
                      //从 map 中寻值
                      if (spuJsonObj != null) {
                                skuInfo.spu_name = spuJsonObj.getString("SPU_NAME")
                      }
                  }
                  skuInfoList.toIterator
              }
          }
          skuInfoRDD
        }else{
          rdd
        }
      }
    }


    //保存到 Hbase
    import org.apache.phoenix.spark._
    skuInfoDstream.foreachRDD{
      rdd=>{
        rdd.saveToPhoenix(
          "GMALL2022_SKU_INFO",
           Seq("ID", "SPU_ID","PRICE","SKU_NAME","TM_ID","CATEGORY3_ID","CREATE_TIME","CATEGORY3_NAME","SPU_NAME","TM_NAME" ),
           new Configuration,
           Some("hadoop102,hadoop103,hadoop104:2181"))
           OffsetManagerUtil.saveOffset(topic,groupId, offsetRanges)

      }
    }

    ssc.start()
    ssc.awaitTermination()


  }
}