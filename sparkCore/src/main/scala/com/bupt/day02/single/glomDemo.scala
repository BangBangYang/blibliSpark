package com.bupt.day02.single

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yangkun
 * @date 2020/10/7 21:06
 * @version 1.0
 */
object glomDemo {
  def main(args: Array[String]): Unit = {
    val list: List[Int] = List(10,20,30,40)


    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("createrdd")
    val sc = new SparkContext(sparkConf)
    val rdd  = sc.parallelize(list,2)
    val rdd2: RDD[List[Int]] = rdd.glom().map(_.toList)
    rdd2.collect.foreach(println)
    sc.stop()
  }
}
