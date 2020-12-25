package com.bupt.day02.kv

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yangkun
 * @date 2020/10/8 19:38
 * @version 1.0
 */
/**
 *aggregateByKey
 * 1. reduceBykey foldByKey 都有预聚合
 * 分区内聚合的逻辑分区间的逻辑是一样的
 * 2. aggregrateByKey 实现了分区聚合逻辑和分区间逻辑的不一样
 *
 */
object aggregateByKeyDemo {
  def main(args: Array[String]): Unit = {
    println(3.max(4))
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("partitionBy")
    val sc = new SparkContext(sparkConf)
    // 计算每个分区的最大值，分区间然后相加
    //1 分区内求最大值 2 分区间求和
    val rdd  = sc.parallelize(List(("a",1),("a",3),("b",10),("a",4),("c",6),("c",7)))
//    val res: RDD[(String, Int)] = rdd.aggregateByKey(Int.MinValue)((u,v)=>u.max(v), (u,v)=>u+v)
    val res: RDD[(String, Int)] = rdd.aggregateByKey(Int.MinValue)(_.max(_),_+_)
    res.collect.foreach(println)

    //计算每个分区内的最大最小值，然后分区间求和
    val res1: RDD[(String, (Int, Int))] = rdd.aggregateByKey((Int.MinValue, Int.MaxValue))({

      case ((mx, mi), v) => (mx.max(v), mi.min(v))
    }, {
      case ((mx1, mi1), (mx2, mi2)) => (mx1 + mx2, mi1 + mi2)
    })
    res1.collect.foreach(println)
     sc.stop()
  }
}
