package com.bupt.day06

import java.sql.DriverManager

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yangkun
 * @date 2020/10/19 21:22
 * @version 1.0
   sc: SparkContext,   Spark程序执行的入口，上下文对象
    getConnection: () => Connection,  获取数据库连接
    sql: String,  执行SQL语句
    lowerBound: Long, 查询的其实位置
    upperBound: Long, 查询的结束位置
    numPartitions: Int,分区数
    mapRow: (ResultSet) => T   对结果集的处理

    注册驱动
    获取连接
    创建数据库操作对象PrepareStatement
    执行SQL
    处理结果集
    关闭连接
 */
object read_mysqlDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("read_mysql").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    //数据库连接4要素
    var driver = "com.mysql.jdbc.Driver"
    var url = "jdbc:mysql://hdp4.buptnsrc.com:3306/test"
    var username = "root"
    var password = "adminzhang123"

    var sql:String = "select * from user where id >= ? and id <= ?"

    val resRDD: JdbcRDD[(Int, String, Int)] = new JdbcRDD(
      sc,
      () => {
        //注册驱动
        Class.forName(driver)
        //获取连接
        DriverManager.getConnection(url, username, password)
      },
      sql,
      2,
      3,
      2,
      rs => (rs.getInt(1), rs.getString(2), rs.getInt(3))
    )
    resRDD.collect().foreach(println)
    sc.stop()
  }
}
