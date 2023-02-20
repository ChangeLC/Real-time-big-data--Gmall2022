package dws
import bean.{OrderDetail, OrderInfo, OrderWide}
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import utill.{MyKafkaSink, MyKafkaUtil, MyRedisUtil, OffsetManagerUtil}

import java.lang
import java.sql.{Connection, DriverManager}
import java.util.Properties
import scala.collection.mutable.ListBuffer

/**
 * Author: Felix
 * Desc: 订单和订单明细双流合并
 */
object OrderWideApp {
  def main(args: Array[String]): Unit = {

    //双流 订单主表 订单明细表 偏移量 双份
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("OrderWideApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val orderInfoGroupId = "dws_order_info_group"
    val orderInfoTopic = "dwd_order_info"
    val orderDetailGroupId = "dws_order_detail_group"
    val orderDetailTopic = "dwd_order_detail"


    //从 redis 中读取偏移量 （启动执行一次）
    val orderInfoOffsetMapForKafka: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(orderInfoTopic, orderInfoGroupId)
    val orderDetailOffsetMapForKafka: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(orderDetailTopic, orderDetailGroupId)


    //根据订单偏移量，从 Kafka 中获取订单数据
    var orderInfoRecordInputDstream: InputDStream[ConsumerRecord[String, String]] = null
    if (orderInfoOffsetMapForKafka != null && orderInfoOffsetMapForKafka.size > 0) {
        //根据是否能取到偏移量来决定如何加载 kafka 流
        orderInfoRecordInputDstream = MyKafkaUtil.getKafkaStream03(orderInfoTopic, ssc, orderInfoOffsetMapForKafka, orderInfoGroupId)
    } else {
        orderInfoRecordInputDstream = MyKafkaUtil.getKafkaStream02(orderInfoTopic, ssc, orderInfoGroupId)
    }


    //根据订单明细偏移量，从 Kafka 中获取订单明细数据
    var orderDetailRecordInputDstream: InputDStream[ConsumerRecord[String, String]] = null
    if (orderDetailOffsetMapForKafka != null && orderDetailOffsetMapForKafka.size > 0) {
      //根据是否能取到偏移量来决定如何加载 kafka 流
      orderDetailRecordInputDstream = MyKafkaUtil.getKafkaStream03(orderDetailTopic, ssc, orderDetailOffsetMapForKafka, orderDetailGroupId)
    } else {
      orderDetailRecordInputDstream = MyKafkaUtil.getKafkaStream02(orderDetailTopic, ssc, orderDetailGroupId)
    }


    //从流中获得本批次的 订单偏移量结束点（每批次执行一次）
    var orderInfoOffsetRanges: Array[OffsetRange] = null
    //周期性储存了当前批次偏移量的变化状态，重要的是偏移量结束点
    val orderInfoInputGetOffsetDstream: DStream[ConsumerRecord[String, String]]= orderInfoRecordInputDstream.transform {
      rdd => //周期性在 driver 中执行
      orderInfoOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }


    //从流中获得本批次的 订单明细偏移量结束点（每批次执行一次）
    var orderDetailOffsetRanges: Array[OffsetRange] = null
    //周期性储存了当前批次偏移量的变化状态，重要的是偏移量结束点
    val orderDetailInputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = orderDetailRecordInputDstream.transform {
      rdd => //周期性在 driver 中执行
      orderDetailOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }


    //提取订单数据
    val orderInfoDstream: DStream[OrderInfo] = orderInfoInputGetOffsetDstream.map {
        record =>{
          val jsonString: String = record.value()
          val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
          orderInfo
        }
      }


    //提取明细数据
    val orderDetailDstream: DStream[OrderDetail] = orderDetailInputGetOffsetDstream.map {
        record =>{
          val jsonString: String = record.value()
          val orderDetail: OrderDetail = JSON.parseObject(jsonString,
            classOf[OrderDetail])
          orderDetail
        }
      }


    //-----------开窗 + 去重完成 join-------------
    //开窗 指定窗口大小和滑动步长
    val orderInfoWindowDstream: DStream[OrderInfo] = orderInfoDstream.window(Seconds(50), Seconds(5))
    val orderDetailWindowDstream: DStream[OrderDetail] = orderDetailDstream.window(Seconds(50), Seconds(5))

    // join
    val orderInfoWithKeyDstream: DStream[(Long, OrderInfo)] =
      orderInfoWindowDstream.map(
        orderInfo=>{
          (orderInfo.id,orderInfo)
        }
      )

    val orderDetailWithKeyDstream: DStream[(Long, OrderDetail)] =
      orderDetailWindowDstream.map(
        orderDetail=>{
          (orderDetail.order_id,orderDetail)
        }
      )

    val joinedDstream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoWithKeyDstream.join(orderDetailWithKeyDstream,4)


    // 去重 数据统一保存到 redis ? type? set api? sadd key ? order_join:[orderId] value ? orderDetailId expire : 60*10
    // sadd 返回如果 0 过滤掉
    val orderWideDstream: DStream[OrderWide] = joinedDstream.mapPartitions {
      tupleItr => {
        val jedis: Jedis = MyRedisUtil.getJedisClient
        val orderWideList: ListBuffer[OrderWide] = ListBuffer[OrderWide]()

        for ((orderId, (orderInfo, orderDetail)) <- tupleItr) {
            val key = "order_join:"+orderId
            val ifNotExisted: lang.Long = jedis.sadd(key, orderDetail.id.toString)
            jedis.expire(key, 600)

            //合并宽表
            if (ifNotExisted == 1L) {
              orderWideList.append(new OrderWide(orderInfo, orderDetail))
            }
        }

        jedis.close()
        orderWideList.toIterator
      }
    }


    // 实付分摊

    val orderWideWithSplitDStream: DStream[OrderWide] = orderWideDstream.mapPartitions {
      orderWideItr =>{
          //建连接
          val jedis: Jedis = MyRedisUtil.getJedisClient
          val orderWideList: List[OrderWide] = orderWideItr.toList
          println("分区 orderIds:" + orderWideList.map(_.order_id).mkString(","))

          //迭代
          for (orderWide <- orderWideList) {
            // 从 Redis 中获取原始金额累计
            // redis type? string key? order_origin_sum:[order_id] value? Σ其他的明细（个数*单价）
            val originSumKey = "order_origin_sum:" + orderWide.order_id
            var orderOriginSum: Double = 0D
            val orderOriginSumStr = jedis.get(originSumKey)

            //从 redis 中取出来的任何值都要进行判空
            if (orderOriginSumStr != null && orderOriginSumStr.size > 0) {
              orderOriginSum = orderOriginSumStr.toDouble
            }

            //从 Reids 中获取分摊金额累计
            // redis type? string key? order_split_sum:[order_id] value? Σ其他的明细的分摊金额
            val splitSumKey = "order_split_sum:" + orderWide.order_id
            var orderSplitSum: Double = 0D
            val orderSplitSumStr: String = jedis.get(splitSumKey)

            if (orderSplitSumStr != null && orderSplitSumStr.size > 0) {
              orderSplitSum = orderSplitSumStr.toDouble
            }

            // 判断是否为最后一笔
            // 如果当前的一笔 个数*单价 == 原始总金额 - Σ其他的明细（个数*单价）
            // 个数 单价 原始金额 可以从 orderWide 取到，明细汇总值已从 Redis 取出
            //如果等式成立 说明该笔明细是最后一笔 （如果当前的一笔 个数*单价== 原始总金额- Σ其他的明细（个数*单价））
            val detailAmount = orderWide.sku_price * orderWide.sku_num


            if (detailAmount == orderWide.original_total_amount - orderOriginSum)
            {
              // 分摊计算公式 :减法公式 分摊金额= 实际付款金额- Σ其他的明细的分摊金额 (减 法，适用最后一笔明细）
              // 实际付款金额 在 orderWide 中，要从 redis 中取得 Σ其他的明细的分摊金额
              orderWide.final_detail_amount = Math.round( (orderWide.final_total_amount - orderSplitSum)*100D)/100D
            } else {
              //如果不成立
              // 分摊计算公式： 乘除法公式： 分摊金额= 实际付款金额 *(个数*单价) / 原始总金额 （乘除法，适用非最后一笔明细)
              // 所有计算要素都在 orderWide 中，直接计算即可
              orderWide.final_detail_amount =
                Math.round( (orderWide.final_total_amount * detailAmount /
                  orderWide.original_total_amount)*100D)/100D
            }
            // 分摊金额计算完成以后
            // 将本次计算的分摊金额 累计到 redis Σ其他的明细的分摊金额
            val newOrderSplitSum = (orderWide.final_detail_amount + orderSplitSum).toString
            jedis.setex(splitSumKey, 600, newOrderSplitSum)

            // 应付金额（单价*个数) 要累计到 Redis Σ其他的明细（个数*单价）
            val newOrderOriginSum = (detailAmount + orderOriginSum).toString
            jedis.setex(originSumKey, 600, newOrderOriginSum)
          }
          // 关闭 redis
          jedis.close()
          //返回一个计算完成的 list 的迭代器
          orderWideList.toIterator
        }
      }

//    保存到clickHouse的时候  打印要关闭  不然将被提前消费  没有数据向clickHouse保存
//    下面代码将rdd转换成DF 有规定rdd转换前如果触发了行动算子（print）,那么数据已经被消费掉了  所以提交行动算子前先进行缓存 che()
//    orderWideWithSplitDStream.print()
//    val value: DStream[String] = orderWideWithSplitDStream.map {
//      orderWide => JSON.toJSONString(orderWide, new SerializeConfig(true))
//
//    }


    //将数据保存到 ClickHouse
    val sparkSession = SparkSession.builder().appName("order_detail_wide_spark_app").getOrCreate()
    import sparkSession.implicits._
    orderWideWithSplitDStream.foreachRDD{
      rdd=>{

        // 这里要加缓存  因为写入到Clickhouse的过程提交的行动算子，数据就没有了，rdd只是计算逻辑的封装，所以这是后的数据已经被消费完了
        // 下面再进行写入到Kafka的算子是没有数据的，所以会不成功
        // 加了缓存之后，就可以重复获取数据
        rdd.cache()

        //将数据保存到 ClickHouse
          val df: DataFrame = rdd.toDF()
          df.write.mode(SaveMode.Append)
            .option("batchsize", "100")
            .option("isolationLevel", "NONE") // 设置事务
            .option("numPartitions", "4") // 设置并发
            .option("driver","ru.yandex.clickhouse.ClickHouseDriver")
            .jdbc("jdbc:clickhouse://hadoop102:8123/default","t_order_wide_2022",new Properties())

        //将数据写回到 Kafka
        rdd.foreach{orderWide=>
          MyKafkaSink.send("dws_order_wide", JSON.toJSONString(orderWide,new SerializeConfig(true)))
        }

        //提交偏移量
          OffsetManagerUtil.saveOffset(orderInfoTopic,orderInfoGroupId,orderInfoOffsetRanges)

          OffsetManagerUtil.saveOffset(orderDetailTopic,orderDetailGroupId,orderDetailOffsetRanges)

      }
    }


    ssc.start()
    ssc.awaitTermination()

  }
}