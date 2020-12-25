package com.bupt.day02.single

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yangkun
 * @date 2020/10/7 21:59
 * @version 1.0
 */

object distinctDemo {
  case class User(age:Int, name:String){
    override def hashCode(): Int = this.age
    override def equals(obj: Any): Boolean = obj match {
      case User(age,_) => this.age == age
      case _ => false
    }
  }
  case class Person(var name:String,var age:Int)
  def main(args: Array[String]): Unit = {
    val list: List[Int] = List(10,20,30,40,10,20,50,1,1,1,1)

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("createrdd")
    val sc = new SparkContext(sparkConf)
    val rdd  = sc.parallelize(list,1)
    val rdd1: RDD[Int] = rdd.distinct()
//    rdd1.collect.foreach(println)
    val users: List[User] = List(User(30,"lisi"),User(10,"zs"),User(10,"oo"),User(20,"lisi"))
//    implicit val ord1:Ordering[User] = new Ordering[User]{
//      override def compare(x: User, y: User) = (x.age - y.age)
//    }
    val rdd2:RDD[User] = sc.parallelize(users,1)
    val res: RDD[User] = rdd2.distinct(1)
    res.foreach(println)


//    rdd2.foreach(println)


    sc.stop()
  }
}
