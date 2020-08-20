package srp.spark.baseline

import srp.spark.utils.{DataUtils, Evaluators}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object WSO {

  def getDev(data: RDD[(String, (String, Double))]): RDD[((String, String), (Double, Int))] = {
    // 物品共现差值矩阵 ((j,i), (devji,cji))
    data.join(data).filter(r => r._2._1._1 != r._2._2._1)
      .map(r => ((r._2._2._1, r._2._1._1), (r._2._2._2 - r._2._1._2, 1))).reduceByKey((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2))
      .map(r => (r._1, (r._2._1 / r._2._2, r._2._2)))
  }

  def getRecommendation(data: RDD[(String, (String, Double))], Dev: RDD[((String, String), (Double, Int))], N: Int): RDD[((String, String), Double)] = {
    // 用户已评分UI对
    val RatedUI = data.map(r => ((r._1, r._2._1), r._2._2))
    val RatedI = data.map(r => (r._2._1, (r._1, r._2._2)))

    // join后为 (i, ((j, devji, cji), (u, ui))) -> ((u,j), ((ui+devji)*cji, cji)) --filter--> reduceByKey
    val Predict = Dev.map(r => (r._1._2, (r._1._1, r._2._1, r._2._2))).join(RatedI)
      .map(r => ((r._2._2._1, r._2._1._1), (r._2._2._2, r._2._1._2, r._2._1._3)))
      .subtractByKey(RatedUI)
      .map(r => (r._1, ((r._2._1 + r._2._2) * r._2._3, r._2._3)))
      .reduceByKey((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2)).map(r => (r._1, r._2._1 / r._2._2))

    println("complete predict")

    val PredictTest = Predict.map(r => r._1)
    val RatedTest = data.map(r => (r._1, r._2._1))
    //println(PredictTest.intersection(RatedTest).count())

    Predict
  }

  def calMAE(test:RDD[(String, (String, Double))], Predict: RDD[((String, String), Double)]): Double = {
    // 中文论文的评价指标(与DL的有些不同)
    // 之前还用subtract(subtractByKey)去根据键值提取交集...这样要几次遍历RDD，好蠢。不如用join
    val res = test.map(r => ((r._1, r._2._1), r._2._2)).join(Predict)
      .map(r => (r._1, math.abs(r._2._1 - r._2._2)))
      .map(r => (0, (1, r._2))).reduceByKey((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2))
      .map(r => r._2._2 / r._2._1)
    res.take(1).head
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("spark://202.38.236.51:7077").setAppName("WSlopeOne")
      .set("spark.executor.memory","3g")
      .set("spark.driver.memory","10g")
      .set("spark.cores.max", "36")
      .set("spark.default.parallelism", "108")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN") // 设置日志级别

    var time = 0
    var MAE_s = 0.0
    var RMSE_s = 0.0
    var Pre_s = 0.0
    var Rec_s = 0.0

    // readData(sc)

    while(time < 10) {

      // 注意：此处选取特定用户数量的数据集，程序需要传入args(0)
      val amount = if(args(0) != null) args(0).toInt else 500
      // 注意：根据选取的不同数量，数据集地址也要改
      val cleanFilterData = sc.textFile("hdfs://202.38.236.51:8020/zhongzhisong/data/retail_data/500user/500userset/ui_gt5-500user-" + time.toString + ".dat").map(line => {
        val splits = line.replace("(", "").replace(")","").split(",")
        (splits(0), (splits(1), splits(2).toDouble, splits(3).toLong))
      })

      // 持久化
      cleanFilterData.persist()

      // data.take(10).foreach(println)

      val TrainAndTest = DataUtils.pretreatData(cleanFilterData)
      // !!!!!!!!!注意检查顺序！
      val train = TrainAndTest._1.map(r => (r._1, (r._2._1, r._2._2)))
      val test = TrainAndTest._2.map(r => (r._1, (r._2._1, r._2._2)))
      // 持久化
      train.persist()
      test.persist()
      //    输出训练与测试数据条数
      println(train.count(), test.count(), train.intersection(test).count(), test.intersection(train).count())

      println(train.collect().length, test.collect().length, test.intersection(train).count(), train.intersection(test).count())

      val Dev = getDev(train)
      println("***getting Recommendation")
      val PredictSet = getRecommendation(train, Dev, 10)

      println("****computing RMSE&MAE")
      val Predict2 = PredictSet.map(r => (r._1._1.toInt, Array((r._1._2.toInt, r._2)))).reduceByKey(_++_)
      val RM = Evaluators.rmseAndMae(Predict2, test.map(r => (r._1.toInt, r._2._1.toInt, r._2._2)))
      RMSE_s = RMSE_s + RM._1
      MAE_s = MAE_s + RM._2
      println(RM)

      println("****computing Precision&Recall(Top10)")
      val Predict3 = PredictSet.map(r => (r._1._1.toInt, Array((r._1._2.toInt, r._2)))).reduceByKey(_++_)
        .map(r => (r._1, r._2.sortBy(ele => ele._2).reverse.take(10)))
      // Predict3.flatMap(r => r._2).collect().foreach(println)
      val PAndR = Evaluators.precisionAndRecall(Predict3, test.map(r => (r._1.toInt, r._2._1.toInt, r._2._2)))
      Pre_s = Pre_s + PAndR._1
      Rec_s = Rec_s + PAndR._2
      println(PAndR)

      time = time + 1

      // 去持久化
      train.unpersist()
      test.unpersist()
      cleanFilterData.unpersist()
    }

    printf("\n***********\n ave MAE: %f\n ave RMSE: %f\n ave Pre: %f\n ave Rec: %f\n", MAE_s/time, RMSE_s/time, Pre_s/time, Rec_s/time)

    sc.stop()
  }
}
