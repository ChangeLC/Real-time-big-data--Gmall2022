package utill
import java.sql.{Connection, DriverManager, ResultSet, ResultSetMetaData, Statement}
import com.alibaba.fastjson.JSONObject
import scala.collection.mutable.ListBuffer
/**
 * Author: Felix
 * Desc: 查询 MySQL 工具类
 */
object MySQLUtil {
  def main(args: Array[String]): Unit = {
    val list: List[ JSONObject] = queryList("select * from offset_2022")
    println(list)
  }

  def queryList(sql:String):List[JSONObject]={

    Class.forName("com.mysql.jdbc.Driver")

    val resultList: ListBuffer[JSONObject] = new ListBuffer[ JSONObject]()

    val conn: Connection = DriverManager.getConnection(
      "jdbc:mysql://hadoop102:3306/gmall2022_rs?characterEncoding=utf-8&useSSL=false",
     "root", "123456")

    val stat: Statement = conn.createStatement

    println(sql)

    val rs: ResultSet = stat.executeQuery(sql)

    val md: ResultSetMetaData = rs.getMetaData

    while ( rs.next ) {
        val rowData = new JSONObject();

        for (i <-1 to md.getColumnCount ) {
          rowData.put(md.getColumnName(i), rs.getObject(i))
        }

        resultList+=rowData
    }
    stat.close()
    conn.close()
    resultList.toList
  }
}