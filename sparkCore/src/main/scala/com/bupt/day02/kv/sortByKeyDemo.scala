package com.bupt.day02.kv

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yangkun
 * @date 2020/10/9 9:39
 * @version 1.0
 */
/**
 * sortByKey
 * 1. 用来排序
 * 2.这个只能用来拍kv形式的RDD
 * 3.这个用的比较少
 * 4. sortBy 这个用的比较多
 *
 */
object sortByKeyDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("partitionBy")
    val sc = new SparkContext(sparkConf)
    // 计算每个分区的最大值，分区间然后相加
    //1 分区内求最大值 2 分区间求和
    val rdd  = sc.parallelize(List((5,"a"),(3,"a"),(8,"b"),(10,"a"),(1,"c"),(2,"c")))

    val res: RDD[(Int, String)] = rdd.sortByKey(false)
    res.collect.foreach(println)
  }

}
