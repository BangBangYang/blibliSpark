package com.bupt.day02.single

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yangkun
 * @date 2020/10/7 16:07
 * @version 1.0
 */
object mapPartitionsWithIndexDemo {
  def main(args: Array[String]): Unit = {
    val array = Array(10,20,30,40)

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("createrdd")
    val sc = new SparkContext(sparkConf)
    val rdd  = sc.parallelize(array,2)
    val res: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex((index, it) => {
      it.map((index, _))
    })
    res
    res.foreach(println)
  }

}
