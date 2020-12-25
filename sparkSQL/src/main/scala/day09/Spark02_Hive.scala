package day09

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @author yangkun
 * @date 2020/10/29 20:54
 * @version 1.0
 */
object Spark02_Hive {
  def main(args: Array[String]): Unit = {
    //创建配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL02_MySQL")
    //创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()

    spark.sql("show tables").show()

    //释放资源
    spark.stop()
  }
}
