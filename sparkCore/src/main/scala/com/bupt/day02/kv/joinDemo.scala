package com.bupt.day02.kv

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yangkun
 * @date 2020/10/9 10:03
 * @version 1.0
 */
/**
 *join
 * 1. 含义和sql差不多，用来连接两个rdd
 * 2. 连接肯定需要连接条件 on a.id = b.id
 *    rdd1.k=rdd2.k
 * 3.sql: 内 左 右 外
 *   rdd: 内 左 右 外
 * mysql 支持fulljoin么？
 * 不支持
 * 如何实现hive的fulljoin
 * leftout union rigjtout
 * leftout union all right where is left.id=null
 */
object joinDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("partitionBy")
    val sc = new SparkContext(sparkConf)

    val rdd  = sc.parallelize(List((5,"a"),(3,"a"),(8,"b"),(10,"a"),(1,"c"),(1,"d")))
    val rdd1 = sc.parallelize(List((1,2),(1,3),(5,6),(8,9)))
    //内连接
//    val res = rdd.join(rdd1)
    //左连接
//    val res = rdd.leftOuterJoin(rdd1)
    //右连接
//    val res = rdd.rightOuterJoin(rdd1)
    //全连接
    val res = rdd.fullOuterJoin(rdd1)
    res.collect.foreach(println)
  }

}
