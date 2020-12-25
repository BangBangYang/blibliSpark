package com.bupt.day06

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

/**
 * @author yangkun
 * @date 2020/10/19 20:57
 * @version 1.0

 */
object readJSONDemo {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("input\\test.json")

    val resRDD: RDD[Option[Any]] = rdd.map(JSON.parseFull)

    resRDD.collect().foreach(println)

    // 关闭连接
    sc.stop()

  }
}
