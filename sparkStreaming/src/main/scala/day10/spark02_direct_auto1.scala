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
 自定的维护偏移量，偏移量维护在checkpiont中
目前我们这个版本，只是指定的检查点，只会将offset放到检查点中，但是并没有从检查点中取，会存在消息丢失
 */
object spark02_direct_auto1 {
  def main(args: Array[String]): Unit = {
    //创建配置文件对象
    val conf: SparkConf = new SparkConf().setAppName("SparkStreaming02_RDDQueue").setMaster("local[*]")

    //创建SparkStreaming上下文环境对象
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))
    //设置检查点目录
    ssc.checkpoint("cp")
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
    ssc.start()
    ssc.awaitTermination()
  }
}
