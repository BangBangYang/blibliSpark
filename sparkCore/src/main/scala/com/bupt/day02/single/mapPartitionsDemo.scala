package com.bupt.day02.single

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yangkun
 * @date 2020/10/7 15:47
 * @version 1.0
 */
object mapPartitionsDemo {
  def main(args: Array[String]): Unit = {
    val array = Array(10,20,30,40,50)

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("createrdd")
    val sc = new SparkContext(sparkConf)
    val rdd  = sc.parallelize(array,2)

    val rdd1: RDD[Int] = rdd.mapPartitions(it => {
      println("abc")
      it.toList //把分区的数据全部加载到内存中，有可能导致分区OOM
      it.map(_ * 2)
    })
    rdd1
    val res = rdd1.collect()
    res.foreach(println)
  }
}
