package com.bupt.day02.single
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yangkun
 * @date 2020/10/7 21:14
 * @version 1.0
 */
object groupByDemo {

  def main(args: Array[String]): Unit = {
    val list: List[Int] = List(3,1,20,30,40)


    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("createrdd")
    val sc = new SparkContext(sparkConf)
    val rdd  = sc.parallelize(list,2)

    val rdd1: RDD[(Int, Iterable[Int])] = rdd.groupBy(x => x%2)
    rdd1.collect.foreach(println)
    val rdd3: RDD[(Int, Int)] = rdd1.map {
      case (i, it) => (i, it.sum)
    }
    rdd3.collect.foreach(println)
    sc.stop()
  }

}
