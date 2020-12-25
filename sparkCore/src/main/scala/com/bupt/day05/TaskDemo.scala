package com.bupt.day05

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yangkun
 * @date 2020/10/15 20:42
 * @version 1.0
 */
object TaskDemo {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //创建RDD
    val dataRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4,1,2),2)

    //聚合
    val resultRDD: RDD[(Int, Int)] = dataRDD.map((_,1)).reduceByKey(_+_)

    // Job：一个Action算子就会生成一个Job；
    //job1打印到控制台
    resultRDD.collect().foreach(println)

    //job2输出到磁盘
    resultRDD.saveAsTextFile("output")

    Thread.sleep(10000000)
    // 关闭连接
    sc.stop()
  }
}
