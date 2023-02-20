package dwd

import bean.OrderDetail
import com.alibaba.fastjson.{JSON, JSONObject}
import com.alibaba.fastjson.serializer.SerializeConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import utill.{MyKafkaSink, MyKafkaUtil, OffsetManagerUtil, PhoenixUtil}

object OrderDetailApp {
  def main(args: Array[String]): Unit = {
    // 加载流 //手动偏移量
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("OrderDetailApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ods_order_detail";
    val groupId = "order_detail_group"

    //从 redis 中读取偏移量
    val offsetMapForKafka: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)

    //通过偏移量到 Kafka 中获取数据
    var recordInputDstream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMapForKafka != null && offsetMapForKafka.size > 0) {
        recordInputDstream = MyKafkaUtil.getKafkaStream03(topic, ssc, offsetMapForKafka, groupId)
    } else {
        recordInputDstream = MyKafkaUtil.getKafkaStream02(topic, ssc, groupId)
    }

    //从流中获得本批次的 偏移量结束点（每批次执行一次）
    //周期性储存了当前批次偏移量的变化状态，重要的是偏移量结束点
    var offsetRanges: Array[OffsetRange] = null
    val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] =
      recordInputDstream.transform {
        rdd => {
          //周期性在 driver 中执行
          offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          rdd
        }
      }

    //提取数据
    val orderDetailDstream: DStream[OrderDetail] = inputGetOffsetDstream.map {
      record =>{
          val jsonString: String = record.value()
          //订单处理 转换成更方便操作的专用样例类
          val orderDetail: OrderDetail = JSON.parseObject(jsonString, classOf[OrderDetail])
          orderDetail
      }
    }

    //订单明细事实表与商品维表数据关联 品牌 分类 spu
    val orderDetailWithSkuDstream: DStream[OrderDetail] =
      orderDetailDstream.mapPartitions {
        orderDetailItr =>{
          val orderDetailList: List[OrderDetail] = orderDetailItr.toList
          if(orderDetailList.size>0) {

            val skuIdList: List[Long] = orderDetailList.map(_.sku_id)
            val sql = s"select id ,tm_id,spu_id,category3_id,tm_name ,spu_name,category3_name from gmall2022_sku_info where id in ('${skuIdList.mkString("','")}')"
            val skuJsonObjList: List[JSONObject] = PhoenixUtil.queryList(sql)

            val skuJsonObjMap: Map[Long, JSONObject] = skuJsonObjList.map(
              skuJsonObj => (skuJsonObj.getLongValue("ID"), skuJsonObj)
            ).toMap


            for (orderDetail <- orderDetailList) {
              val skuJsonObj: JSONObject = skuJsonObjMap.getOrElse(orderDetail.sku_id, null)
              orderDetail.spu_id = skuJsonObj.getLong("SPU_ID")
              orderDetail.spu_name = skuJsonObj.getString("SPU_NAME")
              orderDetail.tm_id = skuJsonObj.getLong("TM_ID")
              orderDetail.tm_name = skuJsonObj.getString("TM_NAME")
              orderDetail.category3_id = skuJsonObj.getLong("CATEGORY3_ID")
              orderDetail.category3_name = skuJsonObj.getString("CATEGORY3_NAME")
            }
          }
          orderDetailList.toIterator
        }

      }
//    orderDetailWithSkuDstream.print(10000)

    //将关联后的订单明细宽表写入到 kafka 中
    orderDetailWithSkuDstream.foreachRDD{
      rdd=>{
        rdd.foreach{
          orderDetail=>{
            MyKafkaSink.send(
              "dwd_order_detail",
              JSON.toJSONString(orderDetail, new SerializeConfig(true)))
          }
        }
        OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
      }
    }



    ssc.start()
    ssc.awaitTermination()

  }

}
