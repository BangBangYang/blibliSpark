package com.bupt.day02.single

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yangkun
 * @date 2020/10/9 14:02
 * @version 1.0
 */
/*
foreach
  1. 遍历 每个元素
  2. 这个遍历操作是executor上完成的
  3. 这个scala的foreach不一样，原来是把数据都拉到驱动，然后使用scala的foreach
  4.map对象：map是转换算子，map也是遍历，会一进一出，foreach是行动算子，一进不出
  5. 一般用于和外部的通信
数据写入mysql
思路一：
  1. 先把数据拉到驱动，然后由驱动统一的的向mysql写入
  2. 建立到mysql的连接(jdbc)
  3. 遍历数组，依次写入
  好处：
    只需要一次mysql连接
   坏处：
    所有数据加载到内存 内存压力大
思路二：
  在executor数据计算完毕，直接写入到mysql
  1. 使用行动算子foreach 分别写入到mysql
  好处：
    遍历一个元素，对内存没有任何压力
   坏处：
     每写一个元素都需要建立一个mysql连接
     mysql连接数会过多，对mysql压力比较大
思路三
    一个分区建立一个到myslq的连接，这个分区的所有数据，可以共用这个连接
     连接数和分区数一致

* */

object foreachDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("foreach")
    val sc = new SparkContext(sparkConf)

    val rdd  = sc.parallelize(List((5,"a"),(3,"a"),(8,"b"),(10,"a"),(1,"c"),(2,"c")))

//    rdd.foreach(x => {
//      //先建立mysql连接
//
//      //写入数据到mysql
//
//      //关闭连接
//      print("x")
//    })

    rdd.foreachPartition(it => {
      //it表示每个分区的所有数据
      //先建立mysql连接

      //写入数据到mysql

      //关闭连接
      print("x")
    })
  }
}
