package day09

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

/**
 * @author yangkun
 * @date 2020/10/29 9:24
 * @version 1.0
 */
object Spark01_SQL_MySQL {
  def main(args: Array[String]): Unit = {
    //创建配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_MySQL")
    //创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    println("========= spark sql read mysql ========")
/*
      println("=== method 1 ==")
      val df: DataFrame = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://hdp4.buptnsrc.com:3306/test?useSSL=false")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "adminzhang123")
      .option("dbtable", "user")
      .load()
    df.show()*/
    // 方式二
//    val df: DataFrame = spark.read.format("jdbc")
//      .options(
//        Map(
//          "url" -> "jdbc:mysql://hdp4.buptnsrc.com:3306/test?user=root&password=adminzhang123",
//          "driver" -> "com.mysql.jdbc.Driver",
//          "dbtable" -> "user"
//        )
//      ).load()
//    df.show()
    //方式三：
    //从MySQL数据库中读取数据  方式3
/*    val props: Properties = new Properties()
    props.setProperty("user","root")
    props.setProperty("password","adminzhang123")
    props.setProperty("driver","com.mysql.jdbc.Driver")
    val df: DataFrame = spark.read.jdbc("jdbc:mysql://hdp4.buptnsrc.com:3306/test","user",props)
    df.show()*/
    //向MySQL数据库中写入数据
    val rdd: RDD[User] = spark.sparkContext.makeRDD(List(User("banzhang",20),User("jingjing",18)))
    //将RDD转换为DF
    //val df: DataFrame = rdd.toDF()
    //df.write.format("jdbc").save()

    //将RDD转换为DS
    val ds: Dataset[User] = rdd.toDS()
    ds.write.format("jdbc")
      .option("url", "jdbc:mysql://hdp4.buptnsrc.com:3306/test")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "adminzhang123")
      .option(JDBCOptions.JDBC_TABLE_NAME, "user")
      .mode(SaveMode.Append)
      .save()
    spark.stop()
  }
}
case class User(name:String,age:Int)