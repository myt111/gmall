package com.atguigu.gmall0921.realtime.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall0921.realtime.utils.{MykafkaUtil, OffsetManagerUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

/**
 * @author mytstart
 * @create 21:32-01-20
 *
 *         手动后置提交偏移量
 * 1. 读取偏移量初始值
 *         从redis中读取偏移量的数据，在redis中以什么样的形式存储
 *         主题=>消费者组=>分区=>offset
 * 2. 把偏移量的起始位置告知给Kafka，让Kafka按照该位置产生数据
 * 3. 从spark driver端获取偏移量的结束点
 * 4. 在保存业务数据之后，提交偏移量结束点到外部容器
 */
object DauApp02 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("dau_app")

    val ssc = new StreamingContext(sparkConf, Seconds(5)) // 1. 业务对时效的需求 2. 处理业务的计算时间 尽量保证周期内可以处理万当前批次
    val topic = "ODS_BASE_LOG"
    val groupid = "dau_app_group"

    // 1. 获取偏移量
    val offsetMap = OffsetManagerUtil.getOffset(topic, groupid)


    var inputDstream: InputDStream[ConsumerRecord[String, String]] = null

    // 2. 从偏移量位置取得数据流，否则从最新位置取得，加载数据
    if (offsetMap == null) {
      inputDstream = MykafkaUtil.getKafkaStream(topic, ssc, groupid)
    } else {
      inputDstream = MykafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupid)
    }

    // val inputDstream: InputDStream[ConsumerRecord[String, String]]

    //4. 结束点
    var offsetRanges:Array[OffsetRange] = null //driver

    //3.从流中把本批次的偏移量结束点存入全局变量
    val inputDstreamWithOffsetDstream = inputDstream.transform { rdd =>
      // dr 在driver中周期性执行 可以写在transform中 或者从rdd中读取数据比如偏移量
      val hasOffsetRanges = rdd.asInstanceOf[HasOffsetRanges] //driver

      offsetRanges = hasOffsetRanges.offsetRanges

      //      rdd.map{
      //        a=>  executor
      //      }
      rdd
    }


    // 把ts转换成日期和小时，方便后续处理
    val jsonStringDstream: DStream[JSONObject] = inputDstreamWithOffsetDstream.map {
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
          for (jsonObj <- jsonObjItr){
            println(jsonObj) // 假设数据存储在数据中
          }

          // 每批次 每分区 exe
        //  OffsetManagerUtil.saveOffset(topic,groupid,offsetRanges)

        }
        // 5. 数据结束后提交偏移量结束点
        // 周期性执行 dri
        OffsetManagerUtil.saveOffset(topic,groupid,offsetRanges)
        println("AAAA")
    }
    // 只执行一次 dri
   // OffsetManagerUtil.saveOffset(topic,groupid,offsetRanges)
    println("BBBB")

    ssc.start()
    ssc.awaitTermination()
  }
}
