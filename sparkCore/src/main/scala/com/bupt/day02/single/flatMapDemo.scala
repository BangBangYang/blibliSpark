package com.bupt.day02.single

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yangkun
 * @date 2020/10/7 20:53
 * @version 1.0
 */
object flatMapDemo {
  def main(args: Array[String]): Unit = {
    val list: List[Int] = List(10,20,30,40)
    val list1: List[Range.Inclusive] = List(1 to 3,5 to 7,8 to 10)

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("createrdd")
    val sc = new SparkContext(sparkConf)
    val rdd  = sc.parallelize(list1,2)

    val rdd1: RDD[Int] = rdd.flatMap(x => x)
//    rdd1.foreach(println)
    val rdd3: RDD[Int] = sc.parallelize(list)
    val rdd4: RDD[Int] = rdd3.flatMap(x => List(x*x,x*x*x))

   val rdd5 =  rdd3.flatMap(x => if(x%2 ==0) List(x,x*x,x*x*x) else List[Int]())
    rdd5.foreach(println)
  }
}
