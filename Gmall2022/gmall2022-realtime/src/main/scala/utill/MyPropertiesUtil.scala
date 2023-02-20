package utill



import java.io.InputStreamReader
import java.nio.charset.StandardCharsets
import java.util.Properties

//读取配置文件工具类
object MyPropertiesUtil {

  def main(args: Array[String]): Unit = {
    val properties = MyPropertiesUtil.load("config.properties")
    println(properties.getProperty("kafka.broker.list"))
  }

  //读取配置信息返回
  def load(propertiesName:String):Properties={
    val properties: Properties = new Properties()
    //加载指定的配置文件，因为是要上传服务器运行，所以不能使用绝对路径，使用反射机制从类加载器中获取
    properties.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertiesName),StandardCharsets.UTF_8))
    properties
  }
}
