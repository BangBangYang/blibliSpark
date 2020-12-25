package com.bupt.day02.kv

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yangkun
 * @date 2020/10/8 17:52
 * @version 1.0
 */
object groupByKeyDemo {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("partitionBy")
    val sc = new SparkContext(sparkConf)
    val rdd  = sc.parallelize(List("hi","yk","hi","yk","spark","spark","spark","hadoop"))
    val prdd: RDD[(String, Int)] = rdd.map((_,1))
    val grdd: RDD[(String, Iterable[Int])] = prdd.groupByKey()
    val res: RDD[(String, Int)] = grdd.mapValues(_.sum)
    res.collect.foreach(println)
  }

}
