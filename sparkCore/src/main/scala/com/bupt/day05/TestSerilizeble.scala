package com.bupt.day05


import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yangkun
 * @date 2020/10/13 20:35
 * @version 1.0
 * Desc:  序列化
 *   -为什么要序列化
 *     因为在Spark程序中，算子相关的操作在Excutor上执行，算子之外的代码在Driver端执行，
 *     在执行有些算子的时候，需要使用到Driver里面定义的数据，这就涉及到了跨进程或者跨节点之间的通讯，
 *     所以要求传递给Excutor中的数组所属的类型必须实现Serializable接口
 *   -如何判断是否实现了序列化接口
 *     在作业job提交之前，其中有一行代码 val cleanF = sc.clean(f)，用于进行闭包检查
 *     之所以叫闭包检查，是因为在当前函数的内部访问了外部函数的变量，属于闭包的形式。
 *     如果算子的参数是函数的形式，都会存在这种情况
 */
object TestSerilizeble {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("serializeTest")

    val sc = new SparkContext(sparkConf)
    val user1 = new User
    user1.name = "lisi"
    val user2: User = new User
    user2.name = "zs"
    val rdd: RDD[User] = sc.parallelize(List(user1,user2))
    rdd.foreach(println)
    sc.stop()
  }

}
class User extends Serializable {
  var name:String = _

  override def toString: String = s"User($name)"
}
