package day11

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author yangkun
 * @date 2020/11/9 21:25
 * @version 1.0
 */
object sparkStreaming02_updateStateByKey {
  def main(args: Array[String]): Unit = {
    //创建配置文件对象   注意：Streaming程序执行至少需要2个线程，所以不能设置为local
    val conf: SparkConf = new SparkConf().setAppName("updateStateByKey").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))
    //设置检查点路径   状态保存在checkpoint中
    ssc.checkpoint("cp")
    val scoketDS: ReceiverInputDStream[String] = ssc.socketTextStream("hdp4.buptnsrc.com",9999)
    val flatDS: DStream[String] = scoketDS.flatMap(_.split(" "))
    val mapDS: DStream[(String, Int)] = flatDS.map((_,1))
    /*
                                     seq中的key对应的value
    (hello,1),(hello,1),(hello,1)===>hello----->(1,1,1)
    * */
    val resDS: DStream[(String, Int)] = mapDS.updateStateByKey(
      //第一个参数：表示的相同的key对应的value组成的数据集合
      //第二个参数：表示的相同的key的缓冲区数据
      (seq: Seq[Int], state: Option[Int]) => {
        //对当前key对应的value进行求和
        //seq.sum
        //获取缓冲区数据
        //state.getOrElse(0)
        Option(seq.sum + state.getOrElse(0))
      }
    )
    resDS.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
