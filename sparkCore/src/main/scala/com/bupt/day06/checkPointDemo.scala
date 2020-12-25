package com.bupt.day06

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yangkun
 * @date 2020/10/18 22:56
 * @version 1.0
 *  1）检查点：是通过将RDD中间结果写入磁盘。
 *  2）为什么要做检查点？
 *  由于血缘依赖过长会造成容错成本过高，这样就不如在中间阶段做检查点容错，如果检查点之后有节点出现问题，可以从检查点开始重做血缘，减少了开销。
 *  3）检查点存储路径：Checkpoint的数据通常是存储在HDFS等容错、高可用的文件系统
 *  4）检查点数据存储格式为：二进制的文件
 *  5）检查点切断血缘：在Checkpoint的过程中，该RDD的所有依赖于父RDD中的信息将全部被移除。
 *  6）检查点触发时间：对RDD进行checkpoint操作并不会马上被执行，必须执行Action操作才能触发。但是检查点为了数据安全，会从血缘关系的最开始执行一遍
 */

/*
检查点和cache区别
1）Cache缓存只是将数据保存起来，不切断血缘依赖。Checkpoint检查点切断血缘依赖。
2）Cache缓存的数据通常存储在磁盘、内存等地方，可靠性低。Checkpoint的数据通常存储在HDFS等容错、高可用的文件系统，可靠性高。
3）建议对checkpoint()的RDD使用Cache缓存，这样checkpoint的job只需从Cache缓存中读取数据即可，否则需要再从头计算一次RDD。
4）如果使用完了缓存，可以通过unpersist（）方法释放缓存

* */

object checkPointDemo {

  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //设置检查点目录
    sc.setCheckpointDir("cp")

    //开发环境，应该将检查点目录设置在hdfs上
    // 设置访问HDFS集群的用户名
    //System.setProperty("HADOOP_USER_NAME","atguigu")
    //sc.setCheckpointDir("hdfs://hadoop202:8020/cp")



    //创建RDD
    val rdd: RDD[String] = sc.makeRDD(List("hello bangzhang","hello jingjing"),2)

    //扁平映射
    val flatMapRDD: RDD[String] = rdd.flatMap(_.split(" "))

    //结构转换
    val mapRDD: RDD[(String, Long)] = flatMapRDD.map {
      word => {
        (word, System.currentTimeMillis())
      }
    }

    //打印血缘关系
    println(mapRDD.toDebugString)


    //在开发环境，一般检查点和缓存配合使用
    mapRDD.cache()

    //设置检查点
    mapRDD.checkpoint()

    //触发行动操作
    mapRDD.collect().foreach(println)

    println("---------------------------------------")

    //打印血缘关系
    println(mapRDD.toDebugString)

    //释放缓存
    //mapRDD.unpersist()

    //触发行动操作
    mapRDD.collect().foreach(println)

    // 关闭连接
    sc.stop()
  }

}
