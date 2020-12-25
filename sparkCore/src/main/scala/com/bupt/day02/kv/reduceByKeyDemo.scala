package com.bupt.day02.kv

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yangkun
 * @date 2020/10/8 17:18
 * @version 1.0
 */
object reduceByKeyDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("partitionBy")
    val sc = new SparkContext(sparkConf)
    val rdd  = sc.parallelize(List("hi","yk","hi","yk","spark","spark","spark","hadoop"))
    val prdd: RDD[(String, Int)] = rdd.map((_,1))
    val res: RDD[(String, Int)] = prdd.reduceByKey(_+_)
    res.collect.foreach(println)
    sc.stop()
  }

}
