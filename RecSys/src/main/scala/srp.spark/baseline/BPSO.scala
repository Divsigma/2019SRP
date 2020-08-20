package srp.spark.baseline

import srp.spark.utils.Evaluators

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.util.Random

object BPSO {
  /**
    * 读取数据
    */
  def readData(sc: SparkContext): RDD[(String, (String, Double, Double))] = {
    val rawData = sc.textFile("hdfs://202.38.236.51:8020/zhongzhisong/data/ml-100k/u.data").map(line => {
      val splits = line.split("\\s+")
      (splits(0), (splits(1), splits(2).toDouble, splits(3).toLong))
    })

    // 时间衰减处理
    // val t0 = 893952000 // 1998/5/1 00:00:00
    val timeDecayData = rawData.map(r => (r._1, (r._2._1, r._2._2, r._2._3)))

    // 归一化处理处理
    val userMaxMin = timeDecayData.map(r => (r._1, ListBuffer(r._2._2))).reduceByKey(_++_).map(r => (r._1, (r._2.max, r._2.min)))

    timeDecayData.join(userMaxMin).map(r => (r._1, (r._2._1._1, (r._2._1._2 - r._2._2._2) / (r._2._2._1 - r._2._2._2), r._2._1._3)))
    //.take(20).foreach(println)
  }

  def getSSDD(data: RDD[(String, (String, Double))]):
  (RDD[(String, (String, Double))], RDD[(String, (String, Double))], RDD[((String, String), (Double, Int))], RDD[((String, String), (Double, Int))]) = {

    // 计算平均值
    val ave = data.map(r => (r._1, (r._2._2, 1))).reduceByKey((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2)).map(r => (r._1, r._2._1 / r._2._2)).collectAsMap()

    // 用户喜好&不喜好物品集
    val SLike = data.filter(r => r._2._2 > ave(r._1))
    val SDislike = data.subtract(SLike)

    // 两极的物品评分平均差值矩阵
    val DevLike = SLike.join(SLike).filter(r => r._2._1._1 != r._2._2._1)
      .map(r => ((r._2._2._1, r._2._1._1), (r._2._2._2 - r._2._1._2, 1))).reduceByKey((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2))
      .map(r => (r._1, (r._2._1 / r._2._2.toDouble, r._2._2)))
    val DevDislike = SDislike.join(SDislike).filter(r => r._2._1._1 != r._2._2._1)
      .map(r => ((r._2._2._1, r._2._1._1), (r._2._2._2 - r._2._1._2, 1))).reduceByKey((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2))
      .map(r => (r._1, (r._2._1 / r._2._2.toDouble, r._2._2)))

    (SLike, SDislike, DevLike, DevDislike)
  }

  def getRecommendation(data: RDD[(String, (String, Double))], SLike: RDD[(String, (String, Double))], SDislike: RDD[(String, (String, Double))], DevLike: RDD[((String, String), (Double, Int))], DevDislike: RDD[((String, String), (Double, Int))], N: Int):
  RDD[((String, String), Double)] = {
    val Rated = data.map(r => ((r._1, r._2._1), r._2._2))
    // 计算like部分的 ((u,j), (sum(pji), sum(cji)))
    // join后为(i, ((u,ui),(j,devji,cji))) -> ((u,j), ((ui + devji)*cji, cji)) = ((u,j), (pji, cji))
    val DevLikeI = DevLike.map(r => (r._1._2, (r._1._1, r._2._1, r._2._2)))
    val Like = SLike.map(r => (r._2._1, (r._1, r._2._2))).join(DevLikeI)
      .map(r => ((r._2._1._1, r._2._2._1), (r._2._1._2, r._2._2._2, r._2._2._3)))
      .subtractByKey(Rated)
      .map(r => (r._1, ((r._2._1 + r._2._2) * r._2._3, r._2._3)))
      .reduceByKey((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2))

    // 同理
    val DevDislikeI = DevDislike.map(r => (r._1._2, (r._1._1, r._2._1, r._2._2)))
    val Dislike = SDislike.map(r => (r._2._1, (r._1, r._2._2))).join(DevDislikeI)
      .map(r => ((r._2._1._1, r._2._2._1), (r._2._1._2, r._2._2._2, r._2._2._3)))
      .subtractByKey(Rated)
      .map(r => (r._1, ((r._2._1 + r._2._2) * r._2._3, r._2._3)))
      .reduceByKey((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2))

    // union并reduce后得到((u,j), (sum(pji), sum(cji))) -> ((u,j), puj) -> (u, [(j, puj)])
    Like.union(Dislike).reduceByKey((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2)).map(r => (r._1, r._2._1 / r._2._2))
  }

  def calMAE(test:RDD[(String, (String, Double))], Predict: RDD[((String, String), Double)]): Double = {
    test.map(r => ((r._1, r._2._1), r._2._2)).join(Predict)
      .map(r => (r._1, math.abs(r._2._1 - r._2._2)))
      .map(r => (0, (1, r._2))).reduceByKey((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2))
      .map(r => r._2._2 / r._2._1).collect()(0)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("spark://202.38.236.51:7077").setAppName("BPSlopeOne")
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

    while(time < 5) {
      val seq = (1 to 943).map(x => x)
      val UserList = Random.shuffle(seq).take(args(0).toInt) // 获取参数
      val data = readData(sc).filter(r => UserList.contains(r._1.toInt))

      val len = data.map(r => (r._1, 1)).reduceByKey(_+_).collectAsMap()
      // 生成打乱后数据集(rdd transformation 的 lazyness，所以要collect/cache)
      // val shuffleData = sc.parallelize(data.map(r => (r._1, ListBuffer( r ))).reduceByKey(_++_).map(r => (r._1, Random.shuffle(r._2))).collect())

      // 按时间排序(时间戳小的在前作为训练集)
      val shuffleData =data.map(r => (r._1, ListBuffer( r ))).reduceByKey(_++_).map(r => (r._1, r._2.sortBy(ele => ele._2._3)))

      val train = shuffleData.flatMap(r => r._2.take(8 * len(r._1) / 10)).map(r => (r._1, (r._2._1, r._2._2)))
      val test = data.map(r => (r._1, (r._2._1, r._2._2))).subtract(train)

      println(data.count(), train.count(), test.count(), test.intersection(train).count(), train.intersection(test).count())

      // 获取两极评分差值矩阵D^(like)(j,i)，D^(dislike)(j,i)
      println("***computing SSDD")
      val SSDD = getSSDD(train)
      println("***getting Recommendation")
      val PredictSet = getRecommendation(train, SSDD._1, SSDD._2, SSDD._3, SSDD._4, 10)
      println("complete predict")

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
    }

    printf("\n***********\n ave MAE: %f\n ave RMSE: %f\n ave Pre: %f\n ave Rec: %f\n", MAE_s/time, RMSE_s/time, Pre_s/time, Rec_s/time)

    sc.stop()
  }
}
