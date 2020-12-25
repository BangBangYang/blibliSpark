package com.bupt.day02.single

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yangkun
 * @date 2020/10/8 9:45
 * @version 1.0
 */
class Person(val age:Int,val name:String) extends Serializable{
  override def toString: String = s"$age"
}
object sortByDemo {

  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("createrdd")
    val sc = new SparkContext(sparkConf)
    val list: List[Int] = List(10,20,30,40,10,20,50,1,1,1,1)
    val list1: List[String] = List("ad","bb","aaaa","bac","ef")
    val rdd  = sc.parallelize(list,3)
//    val rdd1: RDD[Int] = rdd.sortBy(x => x) //升序
//    val rdd1: RDD[Int] = rdd.sortBy(x => x,ascending = false) //降序
//    val rdd1: RDD[Int] = rdd.sortBy(+_) //升序
//    val rdd1:RDD[Int] = rdd.sortBy(-_)  //降序
//    rdd1.collect.foreach(println)
    val rdd_s  = sc.parallelize(list1,3)
//    val rdds1: RDD[String] = rdd_s.sortBy(x => x) //字符串升序
//    val rdds1: RDD[String] = rdd_s.sortBy(_+"")
//    val rdds2: RDD[String] = rdd_s.sortBy(x => x,ascending = false) //字符串降序
//    val rdds1: RDD[String] = rdd_s.sortBy(_ +"",false)
    val lenrdd: RDD[String] = rdd_s.sortBy(_.length) //根据字符串长度排序
    lenrdd.collect.foreach(println)

    val prddsc = sc.parallelize(new Person(20,"lisi")::new Person(10,"zs")::new Person(15,"hi")::Nil)
    implicit val ord:Ordering[Person] = new Ordering[Person]{
      override def compare(x: Person, y: Person): Int = x.age - y.age
    }
    val sortRdd: RDD[Person] = prddsc.sortBy(x=>x)
    sortRdd.collect().foreach(println)

    sc.stop()
  }

}
