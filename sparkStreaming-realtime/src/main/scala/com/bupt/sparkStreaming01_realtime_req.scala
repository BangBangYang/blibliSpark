package com.bupt



import java.text.SimpleDateFormat
import java.util.Date

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
 * @author yangkun
 * @date 2020/11/10 19:30
 * @version 1.0
 *  Desc: 需求：每天每地区热门广告top3
 */
object sparkStreaming01_realtime_req {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("realtime").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))
    ssc.checkpoint("cp")
    //kafka参数声明
    val kafkaParams = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hdp4.buptnsrc.com:6667",
      ConsumerConfig.GROUP_ID_CONFIG -> "yk",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "smallest"
    )
    //创建DS
     val kafkaDS: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,Set("ads-yk"))
    //从kafka的kv值中取value     msg = 1590136353874,华北,北京,103,1
 val mapDS: DStream[String] = kafkaDS.map(_._2)
    //将从kafka获取到的原始数据进行转换  ==>(天_地区_广告,1)
    val mapDS1: DStream[(String, Int)] = mapDS.map(
      line => {
        val fields = line.split(",")
        //获取时间戳
        val timeStamp = fields(0).toLong
        //根据时间戳创建日期对象
        val date: Date = new Date(timeStamp)
        //创建SimpleDataFormat，对日期对象进行转换
        val sdf: SimpleDateFormat = new SimpleDateFormat("yy-MM-dd")
        //将日期对象转换为字符串
        val timeStr: String = sdf.format(date)
        //获取地区
        val area = fields(1)
        //获取广告
        val ad = fields(4)
        //封装元组
        (timeStr + "_" + fields(1) + "_" + fields(4), 1)
      }
    )
    //对每天每地区广告点击数进行聚合处理   (天_地区_广告,sum)
    //注意：这里要统计的是一天的数据，所以要将每一个采集周期的数据都统计，需要传递状态，所以要用udpateStateByKey
    val updateDS: DStream[(String, Int)] = mapDS1.updateStateByKey[Int](
      (seq: Seq[Int], buffer: Option[Int]) => {
        Option(seq.sum + buffer.getOrElse(0))
      }
    )
    //再次对结构进行转换
    val mapDS2: DStream[(String, (String, Int))] = updateDS.map {
      case (key, sum) => {
        val fields: Array[String] = key.split("_")
        (fields(0) + "_" + fields(1), (fields(2), sum))
      }
    }
    //将相同的天和地区放到一组
    val groupDS: DStream[(String, Iterable[(String, Int)])] = mapDS2.groupByKey()
    //对分组周的数据进行排序
    val resDS: DStream[(String, List[(String, Int)])] = groupDS.mapValues(
      datas => {
        datas.toList.sortBy(-_._2).take(3)
      }
    )
    //打印输出结果
    resDS.print()


    ssc.start()
    ssc.awaitTermination()
  }

}
