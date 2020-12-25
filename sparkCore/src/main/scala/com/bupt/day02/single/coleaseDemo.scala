package com.bupt.day02.single

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *coalesce 默认情况用来减少分区，无shuffle
 * coalesce（6，trure）
 * 第二个参数表示是否shuffle，若是true则可以增加分区
 *
 * 注意：如果增加分区一定是shuffle
 * repartition
 * 重新分区，一定shuffle，一般用来增加分区，减少分区不建议使用
 *
 */
object coleaseDemo {

  def main(args: Array[String]): Unit = {
    val list: List[Int] = List(10,20,30,40,10,20,50,1,1,1,1)

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("createrdd")
    val sc = new SparkContext(sparkConf)
    val rdd  = sc.parallelize(list,5)
    val rdd1: RDD[Int] = rdd.coalesce(3)
    println(rdd1.getNumPartitions)

    sc.stop()
  }

}
