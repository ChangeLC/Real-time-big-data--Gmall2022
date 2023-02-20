package utill

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object MyRedisUtil {
    // 定义一个连接池对象
  private var jedisPool:JedisPool = null

  //获取Jedis客户端
  def getJedisClient():Jedis={
    if(jedisPool == null){
      buil()
    }
    jedisPool.getResource
  }

  def main(args: Array[String]): Unit = {
     val jedis = getJedisClient()
    println(jedis.ping())
  }

  // 创建连接池对象
  def buil():Unit = {

    //从配置文件从拿取连接信息
    val config = MyPropertiesUtil.load("config.properties")
    val host = config.getProperty("redis.host")
    val port = config.getProperty("redis.port")
    // 创建配置对象
    val jedisPoolConfig: JedisPoolConfig = new JedisPoolConfig()
    jedisPoolConfig.setMaxTotal(100) //最大连接数
    jedisPoolConfig.setMaxIdle(20) //最大空闲
    jedisPoolConfig.setMinIdle(20) //最小空闲
    jedisPoolConfig.setBlockWhenExhausted(true) //忙碌时是否等待
    jedisPoolConfig.setMaxWaitMillis(5000)//忙碌时等待时长 毫秒
    jedisPoolConfig.setTestOnBorrow(true) //每次获得连接的进行测试

    jedisPool=new JedisPool(jedisPoolConfig,host,port.toInt)
  }



}
