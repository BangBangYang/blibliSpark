package com.bupt

import java.text.SimpleDateFormat
import java.util.Date

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * Author: Felix
  * Date: 2020/5/22
  * Desc: 需求：每天每地区热门广告top3
  */
object RealTime_req1 {
  def main(args: Array[String]): Unit = {
    //创建配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HighKafka")

    //创建SparkStreaming执行的上下文
    val ssc = new StreamingContext(conf, Seconds(3))

    //设置检查点目录
    ssc.sparkContext.setCheckpointDir("D:\\dev\\workspace\\bigdata-0105\\spark-realtime-0105\\cp")

    //kafka参数声明
    val brokers = "hadoop202:9092,hadoop203:9092,hadoop204:9092"
    val topic = "my-ads-0105"
    val group = "bigdata"
    val deserialization = "org.apache.kafka.common.serialization.StringDeserializer"
    val kafkaParams = Map(
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> deserialization,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> deserialization
    )
    //创建DS
    val kafkaDS: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, Set(topic))

    //从kafka的kv值中取value     msg = 1590136353874,华北,北京,103,1
    val dataDS: DStream[String] = kafkaDS.map(_._2)

    //将从kafka获取到的原始数据进行转换  ==>(天_地区_广告,1)
    val mapDS: DStream[(String, Int)] = dataDS.map {
      line => {
        val fields: Array[String] = line.split(",")

        //获取时间戳
        val timeStamp: Long = fields(0).toLong
        //根据时间戳创建日期对象
        val day = new Date(timeStamp)
        //创建SimpleDataFormat，对日期对象进行转换
        val sdf = new SimpleDateFormat("yyyy-MM-dd")
        //将日期对象转换为字符串
        val dayStr: String = sdf.format(day)
        //获取地区
        var area = fields(1)
        //获取广告
        var adv = fields(4)
        //封装元组
        (dayStr + "_" + area + "_" + adv, 1)
      }
    }

    //对每天每地区广告点击数进行聚合处理   (天_地区_广告,sum)
    //注意：这里要统计的是一天的数据，所以要将每一个采集周期的数据都统计，需要传递状态，所以要用udpateStateByKey
    val updateDS: DStream[(String, Int)] = mapDS.updateStateByKey(
      (seq: Seq[Int], buffer: Option[Int]) => {
        Option(seq.sum + buffer.getOrElse(0))
      }
    )

    //再次对结构进行转换
    val mapDS1: DStream[(String, (String, Int))] = updateDS.map {
      //(天_地区_广告,sum)
      case (k, sum) => {
        val fields: Array[String] = k.split("_")
        //(天_地区,(广告,sum))
        (fields(0) + "_" + fields(1), (fields(2), sum))
      }
    }

    //将相同的天和地区放到一组
    val groupDS: DStream[(String, Iterable[(String, Int)])] = mapDS1.groupByKey()

    //对分组周的数据进行排序
    val resDS: DStream[(String, List[(String, Int)])] = groupDS.mapValues {
      datas => {
        datas.toList.sortBy(-_._2).take(3)
      }
    }

    //打印输出结果
    resDS.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
