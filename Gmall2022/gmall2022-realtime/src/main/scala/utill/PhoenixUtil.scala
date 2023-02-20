package utill

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, ResultSetMetaData, Statement}
import com.alibaba.fastjson.JSONObject

import scala.collection.mutable.ListBuffer

// 用于从phonexi中查询数据
// User_id   if_consumerd
//    zs          1

// 返回结果
// {"user_id":"zs","if_consumerd":"1"}

object PhoenixUtil {

  def main(args: Array[String]): Unit = {
    val sql: String = "select * from user_status2022"
    val rslist: List[JSONObject] = queryList(sql)
    println(rslist)
  }
  def queryList(sql:String): List[JSONObject] ={

      val resultList: ListBuffer[JSONObject] = new ListBuffer[ JSONObject]()

      // 注册驱动
      Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")

      // 建立连接
      val conn: Connection = DriverManager.getConnection("jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181")

      // 创建数据库操作对象
      val ps: PreparedStatement = conn.prepareStatement(sql)

      // 执行sql语句
      val rs: ResultSet = ps.executeQuery()
      val rsMetaDate: ResultSetMetaData = rs.getMetaData

      // 处理结果集
      while (rs.next()){

        val userStatusJsonObj: JSONObject = new JSONObject()

        for (i <- 1 to rsMetaDate.getColumnCount) {
          userStatusJsonObj.put(rsMetaDate.getColumnName(i),rs.getObject(i))

        }
        resultList.append(userStatusJsonObj)

      }


      // 关闭资源
      rs.close()
      ps.close()
      conn.close()

      resultList.toList
  }
}
