package srp.spark.utils

import org.apache.spark.rdd.RDD

import scala.collection.mutable

object Evaluators {

  /**
    * 准确率和召回率
    * @param result
    * @param test
    * @return
    */
  def precisionAndRecall(result:RDD[(Int, Array[(Int, Double)])], test:RDD[(Int, Int, Double)]) = {
    val real_sets: RDD[(Int, Array[Int])] = test.map(x=>(x._1,Array(x._2))).reduceByKey(_++_)
    val rec_sets: RDD[(Int, Array[Int])] = result.map(x=>(x._1,x._2.map(_._1)))

    // 前面是实际点击的列表，后面是推荐的列表
    val common_users: RDD[(Int, (Array[Int], Array[Int]))] = real_sets.join(rec_sets)
    // 每个用户推荐命中的物品集
    val common_sets = common_users.map(x=>(x._1,x._2._1.filter(x._2._2.contains(_))))

    // 推荐命中物品的总数量
    val hit = common_sets.map(_._2.length).reduce(_ + _)

    // 每个推荐列表物品数量总和
    val precision_tmp = common_users.map(x=>x._2._2.length).reduce(_ + _)

    // 每个用户实际点集物品数量的综合
    val recall_tmp = common_users.map(x=>x._2._1.length).reduce(_ + _)

    val precision = hit.toDouble / precision_tmp.toDouble

    val recall = hit.toDouble / recall_tmp.toDouble

    (precision, recall)
  }

  /**
    * 覆盖率
    * @param result
    * @param allData
    * @return
    */
  def coverage(result:RDD[(String, Array[(String, Double)])],allData:RDD[(String,String,Double)]) = {

    val all_items = allData.map(_._2).distinct.count()
    val recommend_items = result.flatMap(f=>f._2.map(x=>x._1)).distinct.count()  //获取推荐的电影总数
    //计算覆盖率
    recommend_items.toDouble / all_items.toDouble
  }

  /**
    * RMSE和MAE
    * @param result
    * @param test
    * @return
    */
  def rmseAndMae(result:RDD[(Int, Array[(Int, Double)])], test:RDD[(Int, Int, Double)]): (Double, Double) = {

    val recData = result.flatMap(f=>f._2.map(x=>((f._1,x._1),x._2)))

    val trueData = test.map(f=>((f._1,f._2),f._3))

    val tmp = trueData.join(recData)

    if(tmp.count() == 0) return (-1, -1)

    val square = tmp.map(f=>math.pow(f._2._1 - f._2._2, 2)).reduce(_+_)

    val oneNorm = tmp.map(f=>math.abs(f._2._1 - f._2._2)).reduce(_ + _)

    val rmse = math.sqrt(square.toDouble / tmp.count().toDouble)

    val mae = oneNorm.toDouble / tmp.count().toDouble

    (rmse, mae)

  }

}
