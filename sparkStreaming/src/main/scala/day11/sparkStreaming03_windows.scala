package day11

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author yangkun
 * @date 2020/11/9 21:03
 * @version 1.0
 */
object sparkStreaming03_windows {
  def main(args: Array[String]): Unit = {
    //创建配置文件对象   注意：Streaming程序执行至少需要2个线程，所以不能设置为local
    val conf: SparkConf = new SparkConf().setAppName("SparkStreaming01_WordCount").setMaster("local[*]")

    //创建SparkStreaming程序执行入口对象（上下文环境对象）
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))

    //从指定的端口获取数据
    val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("hdp4.buptnsrc.com",9999)

    val windowsDS: DStream[String] = socketDS.window(Seconds(6),Seconds(3))
    val resDS: DStream[(String, Int)] = windowsDS.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

//    resDS.print()
    //输出数据到指定的文件
    //resDS.saveAsTextFiles("output\\jingjing","liuliu")

    /*resDS.foreachRDD(
      rdd=>{
        //将RDD中的数据保存到MySQL数据库
        rdd.foreachPartition{
          //注册驱动
          //获取连接
          //创建数据库操作对象
          //执行SQL语句
          //处理结果集
          //释放资源
          datas=>{
              //....
          }
        }
      }
    )*/

    //启动采集器
    ssc.start()

    //默认情况下，采集器不能关闭
    //ssc.stop()


    //等待采集结束之后，终止程序
    ssc.awaitTermination()
  }
}
