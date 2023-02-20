package app

import bean.DauInfo
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import utill.{MyESUtil, MyKafkaUtil, MyRedisUtil, OffsetManagerUtil}
import java.lang
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ListBuffer



//日活业务
object DauApp {
  def main(args: Array[String]): Unit = {

    val duaApp: SparkConf = new SparkConf().setMaster("local[4]").setAppName("DuaApp")
    val ssc: StreamingContext = new StreamingContext(duaApp, Seconds(5))
    val topic: String = "gmall2022_start"
    val groupId: String = "gmall_dau_2022"


    // 第一步：根据偏移量读取kafka数据

    //Redis中获取KafKa分区偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)

    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null

    if(offsetMap != null && offsetMap.size>0){
      //如果Redis中存在当前消费组对该主题的消费偏移量信息，那么从执行的偏移量位置开始消费
       kafkaDStream = MyKafkaUtil.getKafkaStream03(topic, ssc, offsetMap, groupId)
    }else{
      // 如果Redis中没有当前消费组对该主题的偏移量信息，那么还是按照配置，从最新位置开始消费
       kafkaDStream = MyKafkaUtil.getKafkaStream02(topic, ssc, groupId)
    }

    // 获取当前采集周期从Kafka中消费的数据的起始偏移量以及结束偏移量值
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform {
      rdd => {
        // 因为kafkaDStream底层封装的是KafkaRDD,混入HasOffsetRanges特质，这个特质中提供了可以获取偏移量范围的方法
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    //    val jsonDStream: DStream[String] = kafkaDStream.map(_.value())
    //    jsonDStream.print()

    // 第二步，对读取到的数据进行处理

    //对数据进行处理
    val jsonDStream: DStream[JSONObject] = offsetDStream.map(
      record => {
        val jsonString: String = record.value() //取到想要的日志数据
        //将json格式的字符串转换为json对象
        val jsonObject: JSONObject = JSON.parseObject(jsonString)
        //获取对象里的时间戳
        val ts: lang.Long = jsonObject.getLong("ts")
        //将时间戳转化为日期小时 2022-08-01 16
        val dateStr: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(ts))
        val dateStrArr: Array[String] = dateStr.split(" ")
        val dt: String = dateStrArr(0)
        val hr: String = dateStrArr(1)
        jsonObject.put("dt", dt)
        jsonObject.put("hr", hr)
        jsonObject

      }
    )

    // jsonDStream.print(1000)

    // 对采集到的数据进行去重操作
    // 通过Redis 对采集到的启动日志进行去重操作  方式一 ： 弊端  每次都要获取一次客户端的连接，连接过于频繁
    // redis 类型 set   key:  dau:2022-08-02  value:mid   expire 3600*24
//    val filteredDStream: DStream[JSONObject] = jsonDStream.filter {
//      jsonObj => {
//
//        // 获取登录日期
//        val dt: String = jsonObj.getString("dt")
//        // 获取设备ID
//        val mid: String = jsonObj.getJSONObject("common").getString("mid")
//        // 获取Jedis客户端
//        val jedis: Jedis = MyRedisUtil.getJedisClient()
//        // 拼接Redis中保存登录信息的Key
//        val dauKey: String = "dau:" + dt
//        // 插入数据到redis  返回0已存在 1未存在
//        val isFirst: lang.Long = jedis.sadd(dauKey, mid)
//        // 设置key的失效时间
//        if(jedis.ttl(dauKey) <0 ) {
//          jedis.expire(dauKey, 3600 * 24)
//        }
//        //关闭连接
//        jedis.close()
//
//        if (isFirst == 1L) {
//          // 说明是第一次登录
//          true
//        } else {
//          // 说明今天已经登录过了
//          false
//        }
//
//      }
//    }

    // 第三步：判断是否首次登录

    // 方式二：以分区的形式对数据进行处理，一个分区获取一次连接
    val filteredDStream: DStream[JSONObject] = jsonDStream.mapPartitions {
      jsonObjItr => {
        //获取 Redis 客户端
        val jedisClient: Jedis = MyRedisUtil.getJedisClient
        //定义当前分区过滤后的数据
        val filteredList: ListBuffer[JSONObject] = new ListBuffer[JSONObject]
        for (jsonObj <- jsonObjItr) {
          //获取当前日期
          val dt: String = jsonObj.getString("dt")
          //获取设备 mid
          val mid: String = jsonObj.getJSONObject("common").getString("mid")
          //拼接向 Redis 放的数据的 key
          val dauKey: String = "dau:" + dt
          //判断 Redis 中是否存在该数据
          val isNew: lang.Long = jedisClient.sadd(dauKey,mid)
          //设置当天的 key 数据失效时间为 24 小时
          //设置Key实效时间
          if (jedisClient.ttl(dauKey) < 0) {
            jedisClient.expire(dauKey, 3600 * 24)
          }
          if (isNew == 1L) {
            //如果 Redis 中不存在，那么将数据添加到新建的 ListBuffer 集合中，实现过滤的效果
            filteredList.append(jsonObj)
          }
        }
        jedisClient.close()
        filteredList.toIterator
      }
    }


//    filteredDStream.print()


    // 将数据持久化到ES
    /*
    在Kibana中先创建好索引模板
    PUT _template/gmall2022_dau_info_template
    {
     "index_patterns": ["gmall2022_dau_info*"],
     "settings": {
         "number_of_shards": 3
     },
     "aliases" : {
         "{index}-query": {},
         "gmall2022_dau_info-query":{}
     },
     "mappings": {
        "_doc":{
            "properties":{
                "mid":{
                    "type":"keyword"
                  },
                "uid":{
                    "type":"keyword"
                 },
                 "ar":{
                    "type":"keyword"
                 },
                 "ch":{
                    "type":"keyword"
                 },
                 "vc":{
                    "type":"keyword"
                 },
                 "dt":{
                    "type":"keyword"
                 },
                 "hr":{
                    "type":"keyword"
                 },
                 "mi":{
                    "type":"keyword"
                 },
                 "ts":{
                    "type":"date"
                 }
           }
        }
     }
  }
    */

     // 第四步：封装样例类，将数据写入ES

    filteredDStream.foreachRDD{
      rdd =>{  //获取 DS 中的 RDD
        rdd.foreachPartition{   //以分区为单位对 RDD 中的数据进行处理，方便批量插入
          jsonObjItr =>{
            var long: lang.Long =null
            val dauList: List[(String,DauInfo)] = jsonObjItr.map {
              jsonObj => {
                //每次处理的是一个 json 对象 将 json 对象封装为样例类
                 long= jsonObj.getLong("ts")
                val commonObject: JSONObject = jsonObj.getJSONObject("common")
                val dauinfo: DauInfo = DauInfo(
                  commonObject.getString("mid"),
                  commonObject.getString("uid"),
                  commonObject.getString("ar"),
                  commonObject.getString("ch"),
                  commonObject.getString("vc"),
                  jsonObj.getString("dt"),
                  jsonObj.getString("hr"),
                  "00", //分钟我们前面没有转换，默认 00
                  jsonObj.getLong("ts")

                )
                (dauinfo.mid,dauinfo)
              }

            }.toList  //将RDD转成集合
            //对分区的数据进行批量处理
            //获取当前日志字符串
            val dt: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date(long))
            MyESUtil.bulkInsert(dauList,"gmall2022_dau_info_"+dt)
          }
        }

        // 提交偏移量到Redis中
        OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)


      }
    }

    ssc.start()
    ssc.awaitTermination()

  }
}
