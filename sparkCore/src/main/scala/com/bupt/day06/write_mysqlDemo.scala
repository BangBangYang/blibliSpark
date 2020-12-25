package com.bupt.day06

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yangkun
 * @date 2020/10/20 10:20
 * @version 1.0
 */
object write_mysqlDemo {
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

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("xingda",30),("ruihao",18)))

    /*
    //在循环体中创建连接对象，每次遍历出RDD中的一个元素，都要创建一个连接对象，效率低，不推荐使用
    rdd.foreach{
      case (name,age)=>{
        //注册驱动
        Class.forName(driver)
        //获取连接
        val conn: Connection = DriverManager.getConnection(url,username,password)
        //声明数据库操作的SQL语句
        var sql:String = "insert into user(name,age) values(?,?)"
        //创建数据库操作对象PrepareStatement
        val ps: PreparedStatement = conn.prepareStatement(sql)
        //给参数赋值
        ps.setString(1,name)
        ps.setInt(2,age)
        //执行SQL
        ps.executeUpdate()
        //关闭连接
        ps.close()
        conn.close()
      }
    }
    */

    /*
    下面这段代码，需要让Ps实现序列化。但是Ps不是我们自己定义的类型，没有办法实现
    //注册驱动
    Class.forName(driver)
    //获取连接
    val conn: Connection = DriverManager.getConnection(url,username,password)
    //声明数据库操作的SQL语句
    var sql:String = "insert into user(name,age) values(?,?)"
    //创建数据库操作对象PrepareStatement
    val ps: PreparedStatement = conn.prepareStatement(sql)

    rdd.foreach{
      case (name,age)=>{
        //给参数赋值
        ps.setString(1,name)
        ps.setInt(2,age)
        //执行SQL
        ps.executeUpdate()
      }
    }
    //关闭连接
    ps.close()
    conn.close()
    */

    //map==>mapPartitions
    rdd.foreachPartition{
      //datas是RDD的一个分区的数据
      datas=>{
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
        datas.foreach{
          case (name,age)=>{
            //给参数赋值
            ps.setString(1,name)
            ps.setInt(2,age)
            //执行SQL
            ps.executeUpdate()
          }
        }

        //关闭连接
        ps.close()
        conn.close()
      }
    }

    // 关闭连接
    sc.stop()
  }
}
