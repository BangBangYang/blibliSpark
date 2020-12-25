package com.bupt.day02.kv

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yangkun
 * @date 2020/10/8 14:58
 * @version 1.0
 */
/**
 * zip 总结
 * 1. 对应分区的元素的个数应该是一样的 2. 分区数应该是一样的
 *
 * zipPartitions
 * 分区数必须相同
 *
 */
object zipDemo {
  def main(args: Array[String]): Unit = {
    val list1 = List(10,20,30,40,50,30)
    val list2 = List(20,40,10,20,30,40)

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("zip")
    val sc = new SparkContext(sparkConf)
    val rdd1  = sc.parallelize(list1)
    val rdd2  = sc.parallelize(list2)
    val res = rdd1.zip(rdd2)
//    res.collect().foreach(println)

    val list3 = List(10,20,30,40,50,60,70,80,90,100)
    val list4 = List(1,2,3,4,5,6)
    val rdd3  = sc.parallelize(list3,2)
    val rdd4  = sc.parallelize(list4,2)
    val res1: RDD[(Int, Int)] = rdd3.zipPartitions(rdd4)((it1, it2) => {
//      it1.zip(it2) //这是scala对应的zip拉链操作，对应分区元素没有要求，长的部分会自动舍弃
      it1.zipAll(it2,111,222)  //第一个元素不够补111，第二个元素不够补222
    })
    res1.collect.foreach(println)

    val res3: RDD[(Int, Long)] = rdd3.zipWithIndex()
    res3.collect().foreach(println)
    sc.stop()

  }

}
