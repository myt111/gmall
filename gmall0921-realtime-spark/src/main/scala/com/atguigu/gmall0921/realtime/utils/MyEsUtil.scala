package com.atguigu.gmall0921.realtime.utils

import java.util

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, Index, Search}
import org.elasticsearch.index.query.MatchQueryBuilder
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.sort.SortOrder


/**
 * @author mytstart
 * @create 11:04-01-26
 */
object MyEsUtil {
  private var factory: JestClientFactory = null

  def getClient: JestClient = {
    if (factory == null) build()
    factory.getObject
  }

  def build(): Unit = {
    factory = new JestClientFactory
    val properties = PropertiesUtil.load("config.properties")
    val serverUrl = properties.getProperty("elasticsearch.server")
    factory.setHttpClientConfig(new HttpClientConfig.Builder(serverUrl)
      .multiThreaded(true)
      .maxTotalConnection(10)
      .connTimeout(10000).build)
  }

  def main(args: Array[String]): Unit = {
    search()

    val jest = getClient
    // source 可以放2中1 class case 2 通用可转json的对象
    val index = new Index.Builder(MovieTest("0104", "电影123")).index("movie_text0921_20210126").`type`("_doc").build()

    jest.execute(index)
    jest.close()
  }

  def search(): Unit = {
    val jest: JestClient = getClient
    val query = "{\\n  \\\"query\\\": {\\n    \\\"match\\\": {\\n      \\\"name\\\": \\\"operation red sea\\\"\\n    }\\n  },\\n  \\\"sort\\\": [\\n    {\\n      \\\"doubanScore\\\": {\\n        \\\"order\\\": \\\"asc\\\"\\n      }\\n    }\\n  ],\\n  \\\"size\\\": 2\\n  , \\\"from\\\": 0\\n  ,\\\"_source\\\": [\\\"name\\\",\\\"doubanScore\\\"]\\n  ,\\\"highlight\\\": { \\\"fields\\\": {\\\"name\\\":{ \\\"pre_tags\\\": \\\"<span color='red'>\\\", \\\"post_tags\\\": \\\"</span>\\\" }}}\\n \\n}"

    val searchSourceBuilder = new SearchSourceBuilder()
    searchSourceBuilder.query(new MatchQueryBuilder("name","operation red sea"))
    searchSourceBuilder.sort("doubanScore",SortOrder.DESC)
    searchSourceBuilder.size(2)
    searchSourceBuilder.from(0)
    searchSourceBuilder.fetchSource(Array("name","doubanScore"),null)


    val search = new Search.Builder(searchSourceBuilder.toString).addIndex("movie_index").addType("movie").build()
    // 接收数据
    val result = jest.execute(search)

    val reList = result.getHits(classOf[util.Map[String, Any]])

    import collection.JavaConverters._
    for (rs <- reList.asScala) {
      println(rs.source)
    } // 1. 专用容器 case class2. 通用容器 map

    jest.close()
  }

  val DEFAULT_TYPE = "_doc"

  // batch bulk
  def saveBulk(indexName: String, docList: List[(String, Any)]): Unit = {
    val jest = getClient


    val bulkBuilder = new Bulk.Builder()
    // 加入很多个单行操作
    for ((id, doc) <- docList) {
      val index = new Index.Builder(doc).id(id).build()
      bulkBuilder.addAction(index)
    }
    // 加入统一保存的索引
    bulkBuilder.defaultIndex(indexName).defaultType(DEFAULT_TYPE)

    val bulk = bulkBuilder.build()
    val items = jest.execute(bulk).getItems
    println("已保存："+items.size()+"条")
    jest.close()
  }

  def save: Unit = {
    val jest = getClient
    // source 可以放2中1 class case 2 通用可转json的对象
    val index = new Index.Builder(MovieTest("0104", "电影123")).index("movie_text0921_20210126").`type`("_doc").build()

    jest.execute(index)
    jest.close()
  }

  case class MovieTest(id: String, movie_name: String)

}
