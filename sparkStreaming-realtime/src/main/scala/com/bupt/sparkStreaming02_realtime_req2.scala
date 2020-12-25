package com.bupt

import java.text.SimpleDateFormat
import java.util.Date

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author yangkun
 * @date 2020/11/10 19:30
 * @version 1.0
 *     -采集周期： 6s
 *     -每3s更新一次：窗口滑动的步长
 *     -各分钟的点击量   ((advId,hhmm),1)
 */
object sparkStreaming02_realtime_req2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("realtime").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))
    ssc.checkpoint("cp")
    val kafkaParams = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hdp4.buptnsrc.com:6667",
      ConsumerConfig.GROUP_ID_CONFIG -> "yk",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "smallest"
    )

     val kafkaDS: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,Set("ads-yk"))
    val mapDS1: DStream[String] = kafkaDS.map(_._2)
    //定义窗口大小以及滑动的步长 窗口大小6s 三秒一更新
    val windowsDS: DStream[String] = mapDS1.window(Seconds(6),Seconds(3))

    val mapDS2: DStream[(String, Int)] = windowsDS.map(
      line => {
        val fields = line.split(",")
        val timeStamp = fields(0).toLong
        val date: Date = new Date(timeStamp)
        val sdf: SimpleDateFormat = new SimpleDateFormat("mm:ss")
        val timeStr: String = sdf.format(date)
        (fields(4) + "_" + timeStr, 1)
      }
    )
    //统计广告6s内每秒个的点击个数
    val resDS: DStream[(String, Int)] = mapDS2.reduceByKey(_+_)
    resDS.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
