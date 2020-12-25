package com.bupt.day02.kv

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * @author yangkun
 * @date 2020/10/8 16:56
 * @version 1.0
 */
/**
 *RDD 代码中没有partitionBy 这个函数，RDD却能调用，paritionBy在PariRDDFunctions中
 * 说明RDD进行了隐式转换
 * RDD的半生对象中存在这个隐式转化的函数
 * implicit def rddToPairRDDFunctions[K, V](rdd: RDD[(K, V)])
 * (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null): PairRDDFunctions[K, V] = {
 * new PairRDDFunctions(rdd)
 * }
 *
 */
object partitionByDemo {
  def main(args: Array[String]): Unit = {
    val list1 = List(10,20,30,40,50,30)


    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("partitionBy")
    val sc = new SparkContext(sparkConf)
    val rdd  = sc.parallelize(list1)

    val rdd1 = rdd.map((_,1))
    println(rdd1.partitioner) //此时没有分区器，分区器是可选的

    val pdd: RDD[(Int, Int)] = rdd1.partitionBy(new HashPartitioner(3))
    println(pdd.partitioner)
    pdd.glom().collect().map(_.toList).foreach(println)
    sc.stop()

  }

}
