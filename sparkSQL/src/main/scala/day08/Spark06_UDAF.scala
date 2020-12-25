package day08

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row, SparkSession, TypedColumn}

/**
 * @author yangkun
 * @date 2020/10/28 15:09
 * @version 1.0
 * 自定义UDAF（弱类型  主要应用在SQL风格的DF查询）
 */
object Spark06_UDAF {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("udaf_demo")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val df: DataFrame = spark.read.json("input\\test.json")
    import spark.implicits._
    /*
      注意：如果是自定义UDAF的强类型，没有办法应用SQL风格DF的查询
      //注册自定义函数
      spark.udf.register("myAvgNew",myAvgNew)
      //创建临时视图
      df.createOrReplaceTempView("user")
      //使用聚合函数进行查询
      spark.sql("select myAvgNew(age) from user").show()
      */
    //将df转换为ds
    val ds: Dataset[User06] = df.as[User06]
    ds.show()
    //创建自定义函数对象
    val myAvgNew: MyAvgNew = new MyAvgNew
    //将自定义函数对象转换为查询列
    val col: TypedColumn[User06, Double] = myAvgNew.toColumn
    //在进行查询的时候，会将查询出来的记录（User01类型）交给自定义的函数进行处理
    ds.select(col).show()


    spark.stop()
  }




}
case class  User06(name:String, age:Long)
case class bufferCount(var sum:Long,var count:Long)
//自定义UDAF函数(强类型)
//* @tparam IN 输入数据类型
//* @tparam BUF 缓存数据类型
//* @tparam OUT 输出结果数据类型
class MyAvgNew extends Aggregator[User06,bufferCount,Double]{
  //对缓存数据进行初始化
  override def zero: bufferCount = {
    bufferCount(0L,0L)
  }
  //对当前分区内数据进行聚合
  override def reduce(b: bufferCount, a: User06): bufferCount = {
    b.count += 1
    b.sum += a.age
    b
  }
  //分区间合并
  override def merge(b1: bufferCount, b2: bufferCount): bufferCount = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }
  //返回计算结果
  override def finish(reduction: bufferCount): Double = reduction.sum.toDouble / reduction.count
  //DataSet的编码以及解码器  ，用于进行序列化，固定写法
  //用户自定义Ref类型  product       系统值类型，根据具体类型进行选择
  override def bufferEncoder: Encoder[bufferCount] = {
    Encoders.product
  }

  override def outputEncoder: Encoder[Double] = {
    Encoders.scalaDouble
  }
}