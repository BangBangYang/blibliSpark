package day08

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * @author yangkun
 * @date 2020/10/28 21:00
 * @version 1.0
 * 自定义UDAF（强类型  主要应用在DSL风格的DS查询）
 */
object Spark07_UDAF {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("udaf_demo")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    //3.读取数据形成RDD ==》 模拟创建两张表 ==》 两份数据RDD
    //这个是员工表的数据
    val sc = spark.sparkContext
    val rdd1: RDD[Person] = sc.parallelize(Array(
      Person("xiaoming",88,"F",6731.2,2),
      Person("laowang",56,"F",8392.1,2),
      Person("caixukun",17,"M",7429.1,2),
      Person("xiaohong",21,"M",781.4,1),
      Person("zhangsan",67,"F",134.1,1),
      Person("lisi",20,"F",1898.3,2),
      Person("wangwu",51,"M",4532.9,1),
      Person("lufei",22,"M",8998.4,1)
    ))

    //这个是部门表的数据
    val rdd2: RDD[Dept] = sc.parallelize(Array(
      Dept(1,"吃饭部"),
      Dept(2,"打牌部")
    ))

    //将RDD转成DataFrame
    import spark.implicits._
    val personDataFrame: DataFrame = rdd1.toDF()  //员工信息表
    val deptDataFrame: DataFrame = rdd2.toDF() //部门信息表

    personDataFrame.show()
    deptDataFrame.show()

    //因为接下来，这两个dataframe会被多次使用，所以建议缓存一下
    personDataFrame.cache()
    deptDataFrame.cache()

    //4.使用DSL语法，来进行练习
    //sql中的select查询
    println("=====================select=========================")
    /*    import org.apache.spark.sql.functions._
        //select name,age,sex from person
        personDataFrame.select("name","age","sex").show()
        personDataFrame.select($"name".alias("name1"),$"age".alias("age1"),$"sex").show()
        personDataFrame.select(col("name").alias("name2"),col("age"),col("sex")).show()
        //注意:当我们使用自定义函数查询的时候，不能直接使用select，而是使用selectExpr来查询
        personDataFrame.selectExpr("name","age","sexToInt(sex) as sex2").show()*/

    //sql中的where条件
    println("====================where/filter==================")
    /*    //select name,age,sex from person where age > '30'
        personDataFrame
          .where("age > 30")
          .select("name","age","sex")
          .show()

        personDataFrame
          .where($"age" > 22)
          .filter("sex = 'M'")
          .select("name","age","sex")
          .show()

        personDataFrame
          .where("age > 22 and sex = 'M' and deptno = 1")
          .show()

        personDataFrame
          .where($"age" > 22 && $"sex" === "M" && $"deptno" === 1)
          .show()*/

    //sql中的排序 ==》 全局排序 、 局部排序
    //hive中全局排序是order by 局部排序是sort by
    println("=====================sort=====================")
    //全局排序 ==> sort  orderby
    /*    personDataFrame
          .sort("sal") //默认从小到大排序
          .select("name","sal")
          .show()

        personDataFrame
          .sort($"sal".desc)  //从大到小排序
          .select("name","sal")
          .show()

        personDataFrame
          .orderBy("sal")
          .select("name","sal")
          .show()*/

    //局部排序 => 全局没有顺序，但是分区中的数据是排序的
    /*   personDataFrame
         .repartition(3)
         .sortWithinPartitions($"age".desc) //从大到小
         .select("name","age")
         .write
         .mode("overwrite")
         .format("json")
         .save("file:///C:\\Users\\ibf\\Desktop\\data")*/

    //sql中的分组之后的聚合操作
    //select avg(sal) from person group by sex
    /*    println("===================groupby aggregate================")
        import org.apache.spark.sql.functions._
        personDataFrame
          .groupBy("sex")
          .agg(
            avg("sal"),
            max("age")
          )
          .show()

        //如果需要修改别名
        personDataFrame
          .groupBy("sex")
          .agg(
            avg($"sal").as("sal1"),
            max($"age").as("age1")
          )
          .show()

        personDataFrame
          .groupBy("sex")
          .agg(
            "sal" -> "avg",
            "age" -> "max"
          )
          .show()*/

    //sql中的join操作 personDataFrame   deptDataFrame
    //select * from emp e join dept d on e.deptno = d.deptno
    println("==================join=====================")
    /*    //inner join
        personDataFrame
          .join(deptDataFrame,"deptno")
          .show()

        //left join
        personDataFrame
          .join(deptDataFrame,Seq("deptno"),"left")
          .show()

        //right join
        personDataFrame
          .join(deptDataFrame,Seq("deptno"),"right")
          .show()*/

    //sql中的窗口函数
    //select empno,ename,sal,row_number() over(partition by empno order by sal desc) from emp
    println("===================窗口函数===============")
    val w = Window.partitionBy("sex").orderBy($"sal".desc)

    //不同性别中薪资最高的前3名
    import org.apache.spark.sql.functions._
    personDataFrame
      .select($"name",$"sex",$"sal",row_number().over(w).as("rn"))
      .where("rn <= 3")
      .show()

    //当程序写完之后，将缓存释放掉
    personDataFrame.unpersist()
    deptDataFrame.unpersist()

    spark.stop()


  }

}
case class User01(name:String,age:Long)
case class Person(name:String,age:Long, sex:String,sal:Double,id:Long)
case class Dept(id:Int,dept:String)
