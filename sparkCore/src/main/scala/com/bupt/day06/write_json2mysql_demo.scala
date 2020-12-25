package com.bupt.day06


import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.util.parsing.json.JSON
/**
 * @author yangkun
 * @date 2020/10/19 22:20
 * @version 1.0
 *          Desc: 向MySQL数据库中写入数据
 *          注册驱动
 *          获取连接
 *          创建数据库操作对象PrepareStatement
 *          执行SQL
 *          处理结果集
 *          关闭连接
 */
object write_json2mysql_demo {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //数据库连接4要素
    var driver = "com.mysql.jdbc.Driver"
    var url = "jdbc:mysql://hdp4.buptnsrc.com:3306/test"
    var username = "root"
    var password = "adminzhang123"

    val rdd: RDD[String] = sc.textFile("input//test.json")
    rdd.collect().foreach(println)
    val resRDD:RDD[Option[Any]] = rdd.map(JSON.parseFull)

    resRDD.foreachPartition(
      datas => {
        //注册驱动
        Class.forName(driver)
        //获取连接
        val conn: Connection = DriverManager.getConnection(url,username,password)
        //声明数据库操作的SQL语句
        var sql:String = "insert into user(name,age) values(?,?)"
        //创建数据库操作对象PrepareStatement
        val ps: PreparedStatement = conn.prepareStatement(sql)

        //对当前分区内的数据，进行遍历
        //注意：这个foreach不是算子了，是集合的方法
        datas.foreach(
          data =>{
            val newdata = data match {
              case Some(map:collection.immutable.Map[String, Any]) => map
            }
            println(newdata)
            //给参数赋值
            ps.setString(1,newdata("name").toString)
            ps.setInt(2,newdata("age").toString.toDouble.toInt)
            //执行SQL
            ps.executeUpdate()
          }
        )
      }
    )
  }
}
