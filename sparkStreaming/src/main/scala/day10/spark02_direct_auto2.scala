package day10

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author yangkun
 * @date 2020/11/6 20:50
 * @version 1.0
 * Desc:   通过DirectAPI连接Kafka数据源，获取数据
 *   自定的维护偏移量，偏移量维护在checkpiont中
 *   修改StreamingContext对象的获取方式，先从检查点获取，如果检查点没有，通过函数创建。会保证数据不丢失
 * 缺点：
 *   1.小文件过多
 *   2.在checkpoint中，只记录最后offset的时间戳，再次启动程序的时候，会从这个时间到当前时间，把所有周期都执行一次
 */
object spark02_direct_auto2 {
  def main(args: Array[String]): Unit = {
    //创建Streaming上下文环境对象
    val ssc: StreamingContext = StreamingContext.getActiveOrCreate("cp",()=>getStreamingContext())
    //设置检查点目录
    ssc.checkpoint("cp")

    ssc.start()
    ssc.awaitTermination()
  }
  def getStreamingContext()={
    val conf: SparkConf = new SparkConf().setAppName("direct2").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))
    //配置kafka参数
    val kafkaParams = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG->"hdp4.buptnsrc.com:6667",
      ConsumerConfig.GROUP_ID_CONFIG->"yk_test"
    )
    val kafkaRDD: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,Set("a"))
    val lineDS: DStream[String] = kafkaRDD.map(_._2)
    //扁平化
    val flatMapDS: DStream[String] = lineDS.flatMap(_.split(" "))

    //结构转换  进行计数
    val mapDS: DStream[(String, Int)] = flatMapDS.map((_,1))

    //聚合
    val reduceDS: DStream[(String, Int)] = mapDS.reduceByKey(_+_)

    //打印输出
    reduceDS.print

    ssc
  }
}
