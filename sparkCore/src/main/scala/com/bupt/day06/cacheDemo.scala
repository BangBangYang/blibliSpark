package com.bupt.day06

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * @author yangkun
 * @date 2020/10/18 21:59
 * @version 1.0
 */
object cacheDemo {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //创建RDD
    val rdd: RDD[String] = sc.makeRDD(List("hello bangzhang","hello jingjing"),2)

    //扁平映射
    val flatMapRDD: RDD[String] = rdd.flatMap(_.split(" "))

    //结构转换
    val mapRDD: RDD[(String, Int)] = flatMapRDD.map {
      word => {
        println("********************")
        (word, 1)
      }
    }

    //打印血缘关系
    println(mapRDD.toDebugString)

    //对RDD的数据进行缓存   底层调用的是persist函数   默认缓存在内存中
    //mapRDD.cache()

    //persist可以接收参数，指定缓存位置
    //注意：虽然叫持久化，但是当应用程序程序执行结束之后，缓存的目录也会被删除
    //mapRDD.persist()
    mapRDD.persist(StorageLevel.DISK_ONLY)

    //触发行动操作
    mapRDD.collect()

    println("---------------------------------------")

    //打印血缘关系
    println(mapRDD.toDebugString)
    //触发行动操作
    mapRDD.collect()

    // 关闭连接
    sc.stop()
  }
}
