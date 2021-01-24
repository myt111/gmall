package com.atguigu.gmall0921.realtime.app


import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall0921.realtime.utils.{MykafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
 * @author mytstart
 * @create 21:32-01-20
 *
 */
object DauApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("dau_app")

    val ssc = new StreamingContext(sparkConf, Seconds(5)) // 1. 业务对时效的需求 2. 处理业务的计算时间 尽量保证周期内可以处理万当前批次
    val topic = "ODS_BASE_LOG"
    val groupid = "dau_app_group"

    val inputDstream: InputDStream[ConsumerRecord[String, String]] = MykafkaUtil.getKafkaStream(topic, ssc, groupid)

    // 把ts转换成日期和小时，方便后续处理
    val jsonStringDstream: DStream[JSONObject] = inputDstream.map {
      record =>
        val jsonString = record.value()

        val jSONObject = JSON.parseObject(jsonString)

        val ts = jSONObject.getLong("ts")
        val dateHourStr = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(ts))

        val dateHour = dateHourStr.split(" ")

        val date = dateHour(0)
        val hour = dateHour(1)

        jSONObject.put("dt", date)
        jSONObject.put("hr", hour)

        jSONObject

    }

    val firstPageJsonObj = jsonStringDstream.filter {
      jsonObj =>
        val pageJson = jsonObj.getJSONObject("page")
        if (pageJson != null) {
          val lastPageId = pageJson.getString("last_page_id")
          if (lastPageId == null || lastPageId.length == 0) {
            true
          } else {
            false
          }

        } else {
          false
        }
    }

    firstPageJsonObj.cache()
    firstPageJsonObj.count().print()

    // 去重 以什么字段进行去重（redis存储已访问列表）
//    val dauDstream = firstPageJsonObj.filter {
//      jsonObj =>
//
//        val mid = jsonObj.getJSONObject("common").getString("mid")
//        val dt = jsonObj.getString("dt")
//        // 提取对象中的mid
//        // 查询列表中是否有mid
//
//        // 设计定义 已访问设备列表
//        // redis? type?(string（每个mid 每天的成为一个key 极端情况下：利于分布式）
//        // set √（可以选择，，把当天已访问mid存入 set key） list（不能去重） zset（不需要排序） hash（多余字段，一个字段mid） key?)
//        // key? dau:2021-01-22 value? mid
//        // expire? 超时时间？ 按天 24小时
//        // 读api? sadd setnx 写api? sadd 自带判断存在exists sismember
//
//        // 把常用一条数据从连接池中取
//        // val jedis = RedisUtil.getJedisClient
//
//        // driver中定义的对象 ex不能直接使用，有条件：1. 必须序列化 2. 不能改值
//        val jedis = RedisUtil.getJedisClient // driver中
//
//
//        val key = "dau:" + dt
//        val isNew = jedis.sadd(key, mid)
//        jedis.expire(key, 3600 * 24)
//
//        // 关闭连接
//        jedis.close()
//
//        if (isNew == 1L) {
//          true
//        } else {
//          false
//        }
//    }

    // 优化： 目的： 减少创建（获取）连接的次数，做成每批次每分区执行一次
    val dauDstream = firstPageJsonObj.mapPartitions {
      jsonObjItr =>

        val jedis = RedisUtil.getJedisClient // driver中 该批次分区执行一次
        val filteredList = ListBuffer[JSONObject]()


        for (jsonObj <- jsonObjItr) {
          // 条为单位处理
          val mid = jsonObj.getJSONObject("common").getString("mid")
          val dt = jsonObj.getString("dt")


          val key = "dau:" + dt
          val isNew = jedis.sadd(key, mid)
          jedis.expire(key, 3600 * 24)


          if (isNew == 1L) {
           filteredList.append(jsonObj)
          }
        }
        // 关闭连接
        jedis.close()
        filteredList.toIterator

    }

    //dauDstream.count().print()

    dauDstream.foreachRDD {
      rdd =>
        rdd.foreachPartition { jsonObjItr =>
          // 存储jsonObjItr

        }

    }
    ssc.start()
    ssc.awaitTermination()
  }
}
