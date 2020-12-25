package com.bupt.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yangkun
 * @date 2020/10/6 20:50
 * @version 1.0
 */
object hello {
  def main(args: Array[String]): Unit = {
    // 创建Spark运行配置对象 打包的时候去掉setMaster 提交的时候用 --master
    val sparkConf = new SparkConf().setAppName("WordCount")//.setMaster("local[*]")

    // 创建Spark上下文环境对象（连接对象）
    val sc : SparkContext = new SparkContext(sparkConf)

    // 读取文件数据
    val fileRDD: RDD[String] = sc.textFile(args(0))

    // 将文件中的数据进行分词
    val wordRDD: RDD[String] = fileRDD.flatMap( _.split(" ") )

    // 转换数据结构 word => (word, 1)
    val word2OneRDD: RDD[(String, Int)] = wordRDD.map((_,1))

    // 将转换结构后的数据按照相同的单词进行分组聚合
    val word2CountRDD: RDD[(String, Int)] = word2OneRDD.reduceByKey(_+_)

    // 将数据聚合结果采集到内存中
    val word2Count: Array[(String, Int)] = word2CountRDD.collect()

    // 打印结果
    word2Count.foreach(println)

    //关闭Spark连接
    sc.stop()

  }
}
///usr/hdp/3.1.0.0-78/spark2/bin/spark-submit --master yarn --deploy-mode client --class com.bupt.day01.hello ./sparkCore-1.0-SNAPSHOT.jar hdfs:///test/a.txt