package com.bupt.day02.single

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yangkun
 * @date 2020/10/9 14:35
 * @version 1.0
 */
/*
序列化
  1. java自带的序列化
    只需要实现java的一个接口:serializable
    好处：
      1. 极其简单，不需要做额外的工作
      2. java自带，用起来方便
     坏处：
      太重
      1.序列化速度慢
      2. 序列化之后的size大
   spark 默认使用的是这种序列化
  2. hadoop没有使用java的序列化
     hadoop 自定义的序列化机智: ... Writeable
方法的序列化:
  把方法所在的类实现接口:Serilazible，这个类创建的对象就可以序列化
属性的序列化：
  1. 和方法的序列化一样
  2. 可以把属性的值存到一个局部变量，然后传递局部变量
有些值无法序列化：
  1. 与外部存储系统的连接不能序列化

3. 支持kyro序列化
  1.指定序列化器: set("spark.serializer",classOf[Kryo].getName) (可以省略，第二步函数内部已经做了)
  2.注册需要使用kryo序列化类型registerKryoClasses(Array(classOf[Searcher]))
  3.即使使用kryo也需要实现Serialize接口
* */

object serDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("foreach")
      .set("spark.serializer",classOf[Kryo].getName)//可以省略
      .registerKryoClasses(Array(classOf[Searcher]))
    val sc = new SparkContext(sparkConf)
    //需求：在RDD中查找出来包含query子字符串的元素
    val rdd  = sc.parallelize(Array("yangkun","hadoop","sparkyangkun","yyangkunooo","zookeeper"))
    //search 在driver创建的
    val searcher = new Searcher("yangkun") //用来查找包含“yangkun”的字符串组成的rdd
    //driver调用
    val res: RDD[String] = searcher.getMatcheRDD1(rdd)
    res.collect.foreach(println)

  }
}
//query为需要查找的子字符串
class Searcher(val query:String) extends Serializable { //extends Serializable
  //判断s中是否包含子字符串query
  def isMatch(s:String)={
    s.contains(query)
  }
  //过滤出包含query字符串的组成的新的RDD
  def getMatcheRDD1(rdd:RDD[String])={

    //filter在driver调用
    //isMatch在executor调用
    rdd.filter(isMatch)

    }
  def getMatcheRDD2(rdd:RDD[String])={
    //query是对象的属性所以需要序列化
    rdd.filter(_.contains(query))
  }
  def getMatcheRDD3(rdd:RDD[String])= {
    val q = query
    rdd.filter(_.contains(q))
  }
}
