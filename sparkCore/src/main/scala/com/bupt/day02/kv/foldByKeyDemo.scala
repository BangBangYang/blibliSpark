package com.bupt.day02.kv

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yangkun
 * @date 2020/10/8 19:05
 * @version 1.0
 */
/**
 * foldByKey:
 * 1. foldByKey也是聚合 和reduceByKey一样，也是聚合
 * 所有的聚合算子都有预聚合
 * 2 多了一个0值功能
 * 3 0值到底参与了多少运算？
 *   只在分区内聚合事有效
 *
 */
object foldByKeyDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("partitionBy")
    val sc = new SparkContext(sparkConf)
    val rdd  = sc.parallelize(List("hi","yk","hi","yk","spark","spark","spark","hadoop"))
    val prdd: RDD[(String, Int)] = rdd.map((_,1))
    val frdd = prdd.foldByKey(0)(_+_)
//    val res: RDD[(String, Int)] = grdd.mapValues(_.sum)
    frdd.collect().foreach(println)
    sc.stop()
  }

}
