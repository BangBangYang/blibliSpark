package com.bupt.day02.kv

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yangkun
 * @date 2020/10/8 20:12
 * @version 1.0
 */
/**
 * 1 reduceByKey
 * 分区内聚合和分区间聚合逻辑一样
 * 2 foldByKey
 * 分区内聚合和分区间逻辑一样，多了一个0值
 * 0值只在分区内使用
 * 3 aggregateByKey
 * 分区内聚合和分区间聚合逻辑不一样
 * 有零值只在分区内使用，零值写死
 * 4 comebineByKey
 * 分区内聚合和分区间聚合逻辑不一样
 * 零值不是写死，根据碰到的每个key的一个value动态生成
 *
 * combineByKey
 *
 *
 */
object combineByKeyDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("partitionBy")
    val sc = new SparkContext(sparkConf)
    // 计算每个分区的最大值，分区间然后相加
    //1 分区内求最大值 2 分区间求和
    val rdd  = sc.parallelize(List(("a",1),("a",3),("b",10),("a",4),("c",6),("c",7)))
    //    val res: RDD[(String, Int)] = rdd.aggregateByKey(Int.MinValue)((u,v)=>u.max(v), (u,v)=>u+v)
    //1、 key求和
    val res = rdd.combineByKey(
      v => v,
      (c:Int,v:Int) => c+v,
      (c1:Int,c2:Int) => c1+c2
    )
    //2、 分区内最大，分区间求和
    val res1: RDD[(String, Int)] = rdd.combineByKey(
      v => v,
      (c: Int, v: Int) => c.max(v),
      (c1: Int, c2: Int) => c1 + c2
    )

    val res3: RDD[(String, Int)] = rdd.combineByKey(
      +_,
      (_: Int).max(_: Int),
      (_: Int) + (_: Int)
    )
//    res3.collect.foreach(println)

    //3 key求平均
    val res4: RDD[(String, (Int, Int))] = rdd.combineByKey(
      v => (v, 1),
      (c: (Int, Int), v: Int) => (c._1 + v, c._2 + 1),
      (c1: (Int, Int), c2: (Int, Int)) => (c1._1 + c2._1, c1._2 + c2._2)
    )
    res4.collect.foreach(println)

  }

}
