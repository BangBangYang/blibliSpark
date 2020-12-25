package day08

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * @author yangkun
 * @date 2020/10/28 15:09
 * @version 1.0
 * 自定义UDAF（弱类型  主要应用在SQL风格的DF查询）
 */
object Spark05_UDAF {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("udaf_demo")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val df: DataFrame = spark.read.json("input\\test.json")
    //创建自定义函数对象
    val avg: myAvg = new myAvg
    //注册自定义函数
    spark.udf.register("myAvg",avg)
    df.show()
    //创建临时视图
    df.createOrReplaceTempView("user")
//    spark.sql("select avg(age) from user").show()
    //使用聚合函数进行查询
    spark.sql("select myAvg(age) from user").show()
    //释放资源
    spark.stop()
  }

}
//自定义UDAF函数(弱类型)
class myAvg extends UserDefinedAggregateFunction{
  //聚合函数的输入数据的类型
  override def inputSchema: StructType = StructType(Array(StructField("age",LongType)))
  //缓存数据的类型
  override def bufferSchema: StructType = {
    StructType(Array(StructField("sum",LongType),StructField("count",LongType)))
  }
  //聚合函数返回的数据类型
  override def dataType: DataType = DoubleType
  //稳定性  默认不处理，直接返回true    相同输入是否会得到相同的输出
  override def deterministic: Boolean = true
  //初始化  缓存设置到初始状态
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    //让缓存中年龄总和归0
    buffer(0) = 0L
    //让缓存中总人数归0
    buffer(1) = 0L
  }
  //更新缓存数据
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    buffer(1) = buffer.getLong(1) + 1
  }
  //分区间的合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0)  = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1)  = buffer1.getLong(1) + buffer2.getLong(1)
  }
  //计算逻辑
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble / buffer.getLong(1)
  }
}