package utill

import bean.DauInfo
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, Get, Index, Search}
import org.elasticsearch.index.query.{BoolQueryBuilder, MatchQueryBuilder, TermQueryBuilder}
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder
import org.elasticsearch.search.sort.SortOrder

import java.util

/*
* Desc:程序中操作ES的工具类
* */
object MyESUtil {

  //声明Jest客户端工厂
  private var jestFactory: JestClientFactory = null;

  //提供获取客户端的方法
  def getJestClient(): JestClient = {
    if (jestFactory == null) {
      //创建Jest客户端的工厂对象
      //调用自定义创建工厂对象的方法
      build()
    }
    //如果工厂对象已经有了，直接获取客户端
    jestFactory.getObject
  }

  //创建工厂对象方法
  def build() = {
    //new一个工厂对象
    jestFactory = new JestClientFactory
    //对工厂对象进行一些配置
    //要传入一个配置对象，配置对象是采用了构造者的设计模式
    jestFactory.setHttpClientConfig(new HttpClientConfig
                                        .Builder("http://hadoop102:9200")
                                        .multiThreaded(true)
                                        .maxTotalConnection(20) //最大连接数
                                        .connTimeout(10000)          //
                                        .readTimeout(10000).build()
    )
  }

  // 向ES中插入单条数据 方式一
  // 将插入文档的数组以json的形式直接传递
  def putIndex() = {
    //获取客户端的连接
    val jestClient = getJestClient()

    //定义执行的source
    var source: String =
      """
        |{
        | "id":200,
        | "name":"incident red sea2",
        | "doubanScore":5.0,
        | "actorList":[
        | {"id":2,"name":"zhang yiming"}
        |]
        |}
        |""".stripMargin

    //创建插入类
    val index = new Index.Builder(source)
      .index("movie_index01")
      .`type`("movie")
      .id("2")
      .build()

    //通过客户端对象操作  execute参数为Action类型，Index是Action接口的实现类
    jestClient.execute(index)

    //关闭连接
    jestClient.close()
  }

  //向ES中插入单条数据  方式二
  // 将要插入的文档封装成一个样例类对象
  def putIndex2() = {
    val jestClient = getJestClient()

    /*{
 "id":400,
 "name":"incident bbd sea",
 "doubanScore":5.0,
 "actorList":[
 {"id":4,"name":"zhang cuishan"}
]
}*/
    val actorList = new util.ArrayList[util.Map[String, Any]]()
    val hashMap = new util.HashMap[String, Any]()
    hashMap.put("id", 3)
    hashMap.put("name", "李若彤")
    actorList.add(hashMap)
    //封装样例类对象
    val movie = Movie(300, "天龙八部", 9.0f, actorList)

    //创建Action实现类 ==>Index
    val index = new Index.Builder(movie)
      .index("movie_index01")
      .`type`("movie")
      .id("3")
      .build()

    jestClient.execute(index)
    jestClient.close()
  }

  case class Movie(id: Long, name: String, doubanScore: Float, actorList: util.List[util.Map[String, Any]]) {

  }

  //根据文档id，从ES中查询出一条记录
  def queryIndexById() = {
    val jestClient = getJestClient()
    val get = new Get.Builder("movie_index01", "3").build()
    val result = jestClient.execute(get)
    println(result.getJsonString)
    jestClient.close()
  }

  //根据指定查询条件，从ES中查询多个文档 方式一
  def queryIndexByCondition() = {
    val jestClient = getJestClient()
    val query =
      """{
        |  "query": {
        |    "bool": {
        |      "must": [
        |        {"match": {
        |          "name": "天龙"
        |        }}
        |      ],
        |      "filter": [
        |        {"term":{"actorList.name.keyword":"李若彤"}}
        |        ]
        |    }
        |  },
        |  "from":0,
        |  "size":20,
        |  "sort":[
        |    {
        |      "doubanScore": {
        |        "order":"desc"
        |      }
        |    }
        |  ],
        |  "highlight": {
        |    "fields": {
        |      "name": {}
        |    }
        |  }
        |}""".stripMargin

    //封装search对象
    val search = new Search.Builder(query)
      .addIndex("movie_index01").build()

    val result = jestClient.execute(search)
    //将查询结果用java进行封装
    val list = result.getHits(classOf[util.Map[String, Any]])
    //将java的List转换成Json的List
    import scala.collection.JavaConverters._
    val list1: List[util.Map[String, Any]] = list.asScala.map(_.source).toList

    println(list1.mkString("\n"))
    jestClient.close()
  }

  //根据指定查询条件，从ES中查询多个文档 方式二
  def queryIndexByCondition2() = {
    val jestClient = getJestClient()

    //searchSourceBuilder用于帮助构建查询的Json格式字符串
    val searchsourcebuilder: SearchSourceBuilder = new SearchSourceBuilder
    val boolQueryBuilder = new BoolQueryBuilder()
    boolQueryBuilder.must(new MatchQueryBuilder("name", "天龙"))
    boolQueryBuilder.filter(new TermQueryBuilder("actorList.name.keyword", "李若彤"))
    searchsourcebuilder.query(boolQueryBuilder)
    searchsourcebuilder.from(0)
    searchsourcebuilder.size(10)
    searchsourcebuilder.sort("doubanScore", SortOrder.ASC)
    searchsourcebuilder.highlighter(new HighlightBuilder().field("name"))
    val query = searchsourcebuilder.toString

    //封装search对象
    val search = new Search.Builder(query)
      .addIndex("movie_index01").build()

    val result = jestClient.execute(search)
    //将查询结果用java进行封装
    val list = result.getHits(classOf[util.Map[String, Any]])
    //将java的List转换成Json的List
    import scala.collection.JavaConverters._
    val list1: List[util.Map[String, Any]] = list.asScala.map(_.source).toList

    println(list1.mkString("\n"))
    jestClient.close()
  }


  /*
  向ES中批量插入数据
   */
  def bulkInsert(infoList: List[(String,Any)], indexName: String): Unit = {

    if (infoList!= null && infoList.size >0){
      //获取客户端
      val client: JestClient = getJestClient()
      //构造批次操作
      val builder: Bulk.Builder = new Bulk.Builder
      //对批量操作的数据进行遍历
      for ((id,dauInfo) <- infoList){
        val index: Index = new Index.Builder(dauInfo)
          .index(indexName)
          .id(id)
          .`type`("_doc")
          .build()

        //将每条数据添加到批量操作中
        builder.addAction(index)
      }
      //Bulk 是 Action 的实现类，主要实现批量操作
      val bulk: Bulk = builder.build()
      //执行批量操作 获取执行结果
      val bulkResult = client.execute(bulk)
      //通过执行结果 获取批量插入的数据
      println("向ES中插入了"+bulkResult.getItems.size()+"条数据")
      //关闭连接
      client.close()
    }

  }

  def main(args: Array[String]): Unit = {
    //    putIndex()
    //    putIndex2()
        queryIndexById()
//        queryIndexByCondition()
//    queryIndexByCondition2()
  }
}
