package com.bupt.day07

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
 * @author yangkun
 * @date 2020/10/21 21:09
 * @version 1.0
 * 需求一：统计热门品类TopN
 */
object TopN_req1 {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("topN1")
    val sc: SparkContext = new SparkContext(conf)
    //1. 读文件
    val rdd: RDD[String] = sc.textFile("input//user_visit_action.txt")
    //2.将读到的数据进行切分，并且将切分的内容封装为UserVisitAction对象
    val userActionRDD: RDD[UserVisitAction] = rdd.map(line => {
      val fields: Array[String] = line.split("_")
      UserVisitAction(
        fields(0),
        fields(1).toLong,
        fields(2),
        fields(3).toLong,
        fields(4),
        fields(5),
        fields(6).toLong,
        fields(7).toLong,
        fields(8),
        fields(9),
        fields(10),
        fields(11),
        fields(12).toLong
      )
    })
    //3.判断当前这条日志记录的是什么行为，并且封装为结果对象    (品类,点击数,下单数,支付数)==>例如：如果是鞋的点击行为  (鞋,1,0,0)
    //(鞋,1,0,0)
    //(保健品,1,0,0)
    //(鞋,0,1,0)
    //(保健品,0,1,0)
    //(鞋,0,0,1)=====>(鞋,1,1,1)
    val infoRDD: RDD[CategoryCountInfo] = userActionRDD.flatMap {
      data => {
        //判断是否为点击行为
        if (data.click_category_id != -1) {
          List(CategoryCountInfo(data.click_category_id.toString, 1, 0, 0))
        }
        //坑：读取的文件应该是null字符串，而不是null对象
        //判断是否为下单行为,如果是下单行为，需要对当前订单中涉及的所有品类Id进行切分
        else if (data.order_category_ids != "null") {
          //定义一个集合，用于存放多个品类id封装的输出结果对象
          val userList = new ListBuffer[CategoryCountInfo]()
          val ids: Array[String] = data.order_category_ids.split(",")
          //对所有品类的id进行遍历
          for (id <- ids) {
            userList.append(CategoryCountInfo(id, 0, 1, 0))
          }
          userList
        }
        //支付
        else if (data.pay_category_ids != "null") {
          //定义一个集合，用于存放多个品类id封装的输出结果对象
          val userList = new ListBuffer[CategoryCountInfo]()
          //对所有品类的id进行遍历
          val ids: Array[String] = data.pay_category_ids.split(",")
          for (id <- ids) {
            userList.append(CategoryCountInfo(id, 0, 1, 0))
          }
          userList
        }
        else {
          Nil
        }
      }
    }
    //4.将相同品类的放到一组
    val groupRDD: RDD[(String, Iterable[CategoryCountInfo])] = infoRDD.groupBy(data => data.categoryId)
    //5.将分组之后的数据进行聚合处理    (鞋,100,90,80)
    val reduceRDD: RDD[(String, CategoryCountInfo)] = groupRDD.mapValues(datas => {
      datas.reduce(
        (c1, c2) => {
          c1.clickCount += c2.clickCount
          c1.orderCount += c2.orderCount
          c1.payCount += c2.payCount
          c1
        }
      )
    })
    //6.对上述RDD的结构进行转换，只保留value部分  ,得到聚合之后的RDD[CategoryCountInfo]
    val mapRDD: RDD[CategoryCountInfo] = reduceRDD.map(_._2)
    //7.对RDD中的数据排序，取前10
    val res: RDD[CategoryCountInfo] = mapRDD.sortBy(item => (item.clickCount,item.orderCount,item.orderCount),false)
    res.take(10).foreach(println)
    sc.stop()

  }

}
//用户访问动作表
case class UserVisitAction(date: String,//用户点击行为的日期
                           user_id: Long,//用户的ID
                           session_id: String,//Session的ID
                           page_id: Long,//某个页面的ID
                           action_time: String,//动作的时间点
                           search_keyword: String,//用户搜索的关键词
                           click_category_id: Long,//某一个商品品类的ID
                           click_product_id: Long,//某一个商品的ID
                           order_category_ids: String,//一次订单中所有品类的ID集合
                           order_product_ids: String,//一次订单中所有商品的ID集合
                           pay_category_ids: String,//一次支付中所有品类的ID集合
                           pay_product_ids: String,//一次支付中所有商品的ID集合
                           city_id: Long)//城市 id


// 输出结果表
case class CategoryCountInfo(categoryId: String,//品类id
                             var clickCount: Long,//点击次数
                             var orderCount: Long,//订单次数
                             var payCount: Long)//支付次数
