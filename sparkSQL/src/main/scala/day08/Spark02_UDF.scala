package day08

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author yangkun
 * @date 2020/10/26 22:12
 * @version 1.0
 */
object Spark02_UDF {
  def main(args: Array[String]): Unit = {
    //创建SparkConf配置文件对象
    val conf: SparkConf = new SparkConf().setAppName("SparkSQL01_Demo").setMaster("local[*]")

    //创建SparkSQL执行的入口点对象  SparkSession
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //注意：spark不是包名，也不是类名，是我们创建的入口对象SparkSession的对象名称
    import spark.implicits._
    spark.udf.register("hi",(x:String) => {"hi:"+x})
    //读取json文件创建DataFrame
    val df: DataFrame = spark.read.json("input\\test.json")
    df.createOrReplaceTempView("user")
    spark.sql("select hi(name),age from user").show()
    spark.stop()

  }
def sayHi(name:String): String={
  "Hi: "+name
}

}
