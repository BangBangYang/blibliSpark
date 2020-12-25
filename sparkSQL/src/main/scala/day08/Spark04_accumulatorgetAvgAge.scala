package day08

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yangkun
 * @date 2020/10/28 11:15
 * @version 1.0
 * 求平均年龄----累加器方式实现
 */
object Spark04_accumulatorgetAvgAge {
  def main(args: Array[String]): Unit = {
    //创建SparkConf配置文件对象
    val conf: SparkConf = new SparkConf().setAppName("SparkSQL04_Demo").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("yangkun",21),("ll",32),("aan",20)))
    //创建累加器对象
    val my = new myAcc
    //注册累加器
    sc.register(my)
    //使用累加器
    rdd.foreach{
      case (name,age) => {
        my.add(age)
      }
    }
    //获得累加器的值
    println(my.value)
  }

}

class myAcc extends AccumulatorV2[Int,Double]{
  var ageSum = 0
  var countSum = 0
  override def isZero: Boolean = ageSum == 0 && countSum == 0

  override def copy(): AccumulatorV2[Int, Double] = {
    var newAcc = new myAcc
    newAcc.ageSum = this.ageSum
    newAcc.countSum = this.countSum
    newAcc
  }

  override def reset(): Unit = {
    ageSum = 0
    countSum = 0
  }

  override def add(v: Int): Unit = {
    this.countSum += 1
    this.ageSum += v
  }

  override def merge(other: AccumulatorV2[Int, Double]): Unit = {
    other match {
      case ot:myAcc => {
        this.ageSum += ot.ageSum
        this.countSum += ot.countSum
      }
      case _ =>
    }
  }

  override def value: Double = ageSum.toDouble / countSum
}
