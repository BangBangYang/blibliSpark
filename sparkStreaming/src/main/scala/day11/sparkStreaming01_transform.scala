package day11

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
 * @author yangkun
 * @date 2020/11/9 21:03
 * @version 1.0
 */
object sparkStreaming01_transform {
  def main(args: Array[String]): Unit = {
    //创建配置文件对象   注意：Streaming程序执行至少需要2个线程，所以不能设置为local
    val conf: SparkConf = new SparkConf().setAppName("SparkStreaming01_WordCount").setMaster("local[*]")

    //创建SparkStreaming程序执行入口对象（上下文环境对象）
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))

    //从指定的端口获取数据
    val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("hdp4.buptnsrc.com",9999)

    //将Ds转换为RDD进行操作
    val resDS: DStream[(String, Int)] = socketDS.transform(
      rdd => {
        val flatMapRDD: RDD[String] = rdd.flatMap(_.split(" "))
        val mapRDD: RDD[(String, Int)] = flatMapRDD.map((_, 1))
        val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
        reduceRDD.sortByKey()
      }
    )

    resDS.print()

    /*
     //扁平化
    val flatMapDS: DStream[String] = socketDS.flatMap(_.split(" "))

    //结构转换  进行计数
    val mapDS: DStream[(String, Int)] = flatMapDS.map((_,1))

    //聚合
    val reduceDS: DStream[(String, Int)] = mapDS.reduceByKey(_+_)
    //打印输出
    reduceDS.print
    */



    //启动采集器
    ssc.start()

    //默认情况下，采集器不能关闭
    //ssc.stop()


    //等待采集结束之后，终止程序
    ssc.awaitTermination()
  }
}
