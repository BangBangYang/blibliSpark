package com.bupt.day07

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
 * @author yangkun
 * @date 2020/10/21 21:09
 * @version 1.0
 * 需求二：统计热门品类topN中，活跃用户的topN
 */
object TopN_req2 {

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
//    res.take(10).foreach(println)
    println("=========================================")
    //1.获取热门品类top10的品类id
    val ids: Array[String] = res.take(10).map(_.categoryId)
    //ids可以进行优化， 因为发送给Excutor中的Task使用，每一个Task都会创建一个副本，所以可以使用广播变量
    val broadcastIds: Broadcast[Array[String]] = sc.broadcast(ids)
    println(ids.mkString(","))

    //2.将原始数据进行过滤（1.保留热门品类 2.只保留点击操作）
    val filterRDD: RDD[UserVisitAction] = userActionRDD.filter(
      action => {
        //只保留点击行为
        if (action.click_category_id != -1) {
          //同时确定是热门品类的点击
          //坑   集合数据为字符串类型，id是Long类型，需要进行转换
          ids.contains(action.click_category_id.toString)
        } else {
          false
        }
      }
    )
    //3.对session的点击数进行转换 (category_session,1)
    val mapRDD1: RDD[(String, Int)] = filterRDD.map(action =>(action.click_category_id+"_"+action.session_id,1))
    //4.对session的点击数进行统计 (category_session,sum)
    val reduceRDD1: RDD[(String, Int)] = mapRDD1.reduceByKey(_+_)
    //5.将统计聚合的结果进行转换  (category,(session,sum))
    val mapRDD2: RDD[(String, (String, Int))] = reduceRDD1.map {
      case (categoryIdAndSession, sum) => (categoryIdAndSession.split("_")(0), (categoryIdAndSession.split("_")(1), sum))

    }
    //6.将转换后的结构按照品类进行分组 (category,Iterator[(session,sum)])
    val groupRDD2: RDD[(String, Iterable[(String, Int)])] = mapRDD2.groupByKey()
    //7.对分组后的数据降序 取前10
    val resRDD: RDD[(String, List[(String, Int)])] = groupRDD2.mapValues {
      datas => {
        datas.toList.sortWith {
          case (left, right) => {
            left._2 > right._2
          }
        }.take(10)
      }
    }
    resRDD.foreach(println)

    sc.stop()

  }

}
////用户访问动作表
//case class UserVisitAction(date: String,//用户点击行为的日期
//                           user_id: Long,//用户的ID
//                           session_id: String,//Session的ID
//                           page_id: Long,//某个页面的ID
//                           action_time: String,//动作的时间点
//                           search_keyword: String,//用户搜索的关键词
//                           click_category_id: Long,//某一个商品品类的ID
//                           click_product_id: Long,//某一个商品的ID
//                           order_category_ids: String,//一次订单中所有品类的ID集合
//                           order_product_ids: String,//一次订单中所有商品的ID集合
//                           pay_category_ids: String,//一次支付中所有品类的ID集合
//                           pay_product_ids: String,//一次支付中所有商品的ID集合
//                           city_id: Long)//城市 id
//
//
//// 输出结果表
//case class CategoryCountInfo(categoryId: String,//品类id
//                             var clickCount: Long,//点击次数
//                             var orderCount: Long,//订单次数
//                             var payCount: Long)//支付次数
