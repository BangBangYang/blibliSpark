package com.bupt.day02.single

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yangkun
 * @date 2020/10/7 15:20
 * @version 1.0
 */
object CreateRDD {
  def main(args: Array[String]): Unit = {
    val array = Array(10,20,30,40)

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("createrdd")
    val sc = new SparkContext(sparkConf)
    val rdd  = sc.parallelize(array)
    val res = rdd.collect()
    res.foreach(println)


    sc.stop()


  }

}
