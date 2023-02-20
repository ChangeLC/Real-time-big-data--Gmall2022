package dim

import bean.UserInfo

import java.text.SimpleDateFormat
import java.util
import com.alibaba.fastjson.JSON
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utill.{MyKafkaUtil, OffsetManagerUtil}

object UserInfoApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("UserInfoApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ods_user_info";
    val groupId = "gmall_user_info_group"


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
    val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = inputDstream.transform {
          rdd =>{
            offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            rdd
          }
      }


    val userInfoDstream: DStream[UserInfo] = inputGetOffsetDstream.map {
          record =>{
                    val userInfoJsonStr: String = record.value()
                    val userInfo: UserInfo = JSON.parseObject(userInfoJsonStr, classOf[UserInfo])
                    //把生日转成年龄
                    val formattor = new SimpleDateFormat("yyyy-MM-dd")
                    val date: util.Date = formattor.parse(userInfo.birthday)
                    val curTs: Long = System.currentTimeMillis()
                    val betweenMs = curTs - date.getTime
                    val age = betweenMs/1000L/60L/60L/24L/365L

                    if(age<20){
                      userInfo.age_group="20 岁及以下"
                    }else if(age>30){
                      userInfo.age_group="30 岁以上"
                    }else{
                      userInfo.age_group="21 岁到 30 岁"
                    }

                    if(userInfo.gender=="M"){
                      userInfo.gender_name="男"
                    }else{
                      userInfo.gender_name="女"
                    }
                    userInfo
          }
    }


    userInfoDstream.foreachRDD{
        rdd=>{
            import org.apache.phoenix.spark._
            rdd.saveToPhoenix(
                  "GMALL2022_USER_INFO",
                  Seq("ID", "USER_LEVEL", "BIRTHDAY", "GENDER","AGE_GROUP","GENDER_NAME")
                  ,new Configuration,Some("hadoop102,hadoop103,hadoop104:2181")
            )
            OffsetManagerUtil.saveOffset(groupId, topic, offsetRanges)
        }

    }

    ssc.start()
    ssc.awaitTermination()
  }
}