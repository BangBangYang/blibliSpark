package day08

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yangkun
 * @date 2020/10/28 11:15
 * @version 1.0
 * 求平均年龄----RDD算子方式实现
 */
object Spark03_RDDgetAvgAge {
  def main(args: Array[String]): Unit = {
    //创建SparkConf配置文件对象
    val conf: SparkConf = new SparkConf().setAppName("SparkSQL01_Demo").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("yangkun",21),("ll",32),("aan",20)))
    val mapRDD: RDD[(Int, Int)] = rdd.map {
      case (name, age) => (age, 1)
    }
    val res: (Int, Int) = mapRDD.reduce(
      (t1, t2) => {
        (t1._1 + t2._1, t2._2 + t1._2)
      }
    )
    println(res._1 / res._2)
    sc.stop()
  }

}
