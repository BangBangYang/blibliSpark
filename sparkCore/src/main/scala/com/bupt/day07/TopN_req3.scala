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
object TopN_req3 {

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
    val pageIdRDD: RDD[(Long, Int)] = userActionRDD.map(action => (action.page_id,1))
    val sumRDD: RDD[(Long, Int)] = pageIdRDD.reduceByKey(_+_)
    val fmIds: Map[Long, Int] = sumRDD.collect().toMap
    val sessionRDD: RDD[(String, Iterable[UserVisitAction])] = userActionRDD.groupBy(_.session_id)
    val pageFlowsRDD: RDD[(String, List[(String, Int)])] = sessionRDD.mapValues {
      datas => {
        val actionSortedList: List[UserVisitAction] = datas.toList.sortWith {
          case (left, right) => left.action_time < right.action_time
        }
        val pageIds: List[Long] = actionSortedList.map(_.page_id)
        val pageFlows: List[(Long, Long)] = pageIds.zip(pageIds.tail)
        pageFlows.map {
          case (pageId1, pageId2) => (pageId1 + "-" + pageId2, 1)
        }

      }
    }
//    pageFlowsRDD.take(10).foreach(println)
    // 3.6 将每一个会话的页面跳转统计完毕之后，没有必要保留会话信息了，所以对上述RDD的结构进行转换
    //只保留页面跳转以及计数
    val pageFlowMapRDD: RDD[(String, Int)] = pageFlowsRDD.map(_._2).flatMap(list=>list)

    //3.7 对页面跳转情况进行聚合操作
    val pageAToPageBSumRDD: RDD[(String, Int)] = pageFlowMapRDD.reduceByKey(_+_)

    //4.页面单跳转换率计算
    pageAToPageBSumRDD.foreach{
      //(pageA-pageB,sum)
      case (pageFlow,fz)=>{
        val pageIds: Array[String] = pageFlow.split("-")
        //获取分母页面id
        val fmPageId: Int = pageIds(0).toInt
        //根据分母页面id，获取分母页面总访问数
        val fmSum: Int = fmIds.getOrElse(fmPageId,1)
        //转换率
        println(pageFlow +"--->" + fz.toDouble / fmSum)
      }
    }
    // 关闭连接

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
