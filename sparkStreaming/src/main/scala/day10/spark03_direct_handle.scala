package day10

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}

/**
 * @author yangkun
 * @date 2020/11/6 21:20
 * @version 1.0
 */
object spark03_direct_handle {
  def main(args: Array[String]): Unit = {
    //创建配置文件对象
    val conf: SparkConf = new SparkConf().setAppName("direct_handle").setMaster("local[*]")

    //创建SparkStreaming上下文环境对象
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))

    //配置kafka参数
    val kafkaParams = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG->"hdp4.buptnsrc.com:6667",
      ConsumerConfig.GROUP_ID_CONFIG->"yk_test"
    )
    //获取上一次消费的位置（偏移量）
    //实际项目中，为了保证数据精准一致性，我们对数据进行消费处理之后，将偏移量保存在有事务的存储中， 如MySQL
    val offsets = Map(TopicAndPartition("a",0)->10L)
    val kafkaRDD: InputDStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](
      ssc,
      kafkaParams,
      offsets,
      (m: MessageAndMetadata[String, String]) => m.message()
    )
    //消费完毕之后，对偏移量offset进行更新
    var offsetRanges = Array.empty[OffsetRange]
    kafkaRDD.transform(
      //rdd是kafkaRDD类型，但是kafkaRDD是私有类型的且实现了HashOffsetRanges接口，因此将rdd转成HashOffsetRanges然后得到offsetRanges
      rdd => {
           offsetRanges= rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          rdd
        }
    ).foreachRDD{
       rdd => {
         for(o <- offsetRanges) {
           println(o.topic,o.partition,o.fromOffset,o.untilOffset)
         }
       }
    }



//    val lineDS: DStream[String] = kafkaRDD.map(_._1)
    //扁平化
    val flatMapDS: DStream[String] = kafkaRDD.flatMap(_.split(" "))

    //结构转换  进行计数
    val mapDS: DStream[(String, Int)] = flatMapDS.map((_,1))

    //聚合
    val reduceDS: DStream[(String, Int)] = mapDS.reduceByKey(_+_)

    //打印输出
    reduceDS.print
    ssc.start()
    ssc.awaitTermination()
  }
}
