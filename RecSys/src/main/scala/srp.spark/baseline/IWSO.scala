package srp.spark.baseline

import srp.spark.utils.{DataUtils, Evaluators}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object IWSO {

  /**
    * 计算物品间Person系数
    * @param data
    * @return
    */
  def getPersonSimilarity(data: RDD[(String,String,Double)],threshold:Double): RDD[((String, String), Double)] = {

    val user_item_score: RDD[(String, (String, Double))] = data.map(x=>(x._1,(x._2,x._3)))

    // 获取每个物品的得分均值
    val item_means_score = user_item_score.map(x=>(x._2._1,(x._2._2,1)))
      .reduceByKey((a,b)=>(a._1 + b._1,a._2 + b._2))
      .map(x=>(x._1,x._2._1 / x._2._2.toDouble)).collectAsMap()

    /*
     * 获取物品对Person相似度(分子分母-物品得分均值)各部件
     */
    val item_pairs_score: RDD[((String, String), (Double, Double, Double))] = user_item_score.join(user_item_score)
      .filter(f=>f._2._1._1 != f._2._2._1)
      .map(x=>((x._2._1._1,x._2._2._1),(x._2._1._2 - item_means_score(x._2._1._1),x._2._2._2 - item_means_score(x._2._2._1))))
      .map(x=>(x._1,(x._2._1 * x._2._2,math.pow(x._2._1, 2), math.pow(x._2._2, 2))))


    // 将符合阈值条件的相似度筛选出来
    val item_pairs_sim = item_pairs_score.reduceByKey((a,b)=>(a._1 + b._1,a._2 + b._2,a._3 + b._3))
      .map(x=>(x._1,x._2._1 / math.sqrt(x._2._2 * x._2._3)))
      .filter(_._2 > threshold)

    item_pairs_sim
  }

  /**
    * 结合物品相似度的dev公式
    * @param data
    * @param sim_scores
    * @return
    */
  def getAdjustDev(data: RDD[(String, (String, Double))],sim_scores: RDD[((String, String), Double)]): RDD[((String, String), (Double, Double))] = {

    // 物品共现差值矩阵 ((i,j), (devij,sim(i,j)))
    data.join(data).filter(r => r._2._1._1 != r._2._2._1)
      .map(r => ((r._2._2._1, r._2._1._1), (r._2._2._2 - r._2._1._2, 1))).reduceByKey((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2))
      .map(r => (r._1, r._2._1 / r._2._2))
      .join(sim_scores)
  }

  /**
    * 结合物品相似度求预测评分
    * IWSO dev公式不修正，但是加权Slope One 的Predict加上物品的Person相关系数进行转换
    * @param data
    * @param Dev
    * @return
    */
  def getIWSORecommendation(data: RDD[(String, (String, Double))], Dev: RDD[((String, String), (Double, Double))],item_sim: RDD[((String, String), Double)], threshold:Double): RDD[((String, String), Double)] = {
    // 用户已评分UI对((uid,iid),rating)
    val RatedUI = data.map(r => ((r._1, r._2._1), r._2._2))
    //(iid,(uid,rating))
    val RatedI: RDD[(String, (String, Double))] = data.map(r => (r._2._1, (r._1, r._2._2)))

    val Predict = Dev.map(x=>(x._1._2, (x._1._1, x._2._1, x._2._2))).join(RatedI)
      .map(r => ((r._2._2._1, r._2._1._1), (r._2._2._2, r._2._1._2, r._2._1._3)))
      .subtractByKey(RatedUI)
      .map(r => (r._1, ((r._2._1 + r._2._2) * r._2._3, r._2._3)))
      .reduceByKey((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2))
      .filter(_._2._2 != 0)
      .map(x => (x._1, x._2._1 / x._2._2))

    println("complete predict")

    Predict
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("IWSO")
//      .master("spark://202.38.236.51:7077")
      .master("local[6]")
//      .config("spark.executor.memory","3g")
//      .config("spark.cores.max","36")
//      .config("spark.driver.memory","10g")
//      .config("spark.locality.wait","10")  //本地化等待时长
//      .config("spark.locality.wait.node","8")  //节点内等待时长，默认3s
//      .config("spark.locality.wait.rack","5")  //机架内等待时长，默认3s
//      .config("spark.storage.memoryFraction","0.7")
//      .config("spark.default.parallelism","18")
//      .config("spark.default.parallelism","108")
//      .config("spark.memory.offheap.enable","true")
//      .config("spark.memory.offheap.enable.size","10g")
//      .config("spark.shuffle.consolidateFiles","true")  //开启HashShuffleManager优化机制
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN") // 设置日志级别

    var time = 0
    var MAE_s = 0.0
    var RMSE_s = 0.0
    var Pre_s = 0.0
    var Rec_s = 0.0

    val threshold = 0.0

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

      // 获取物品间相似度
      val item_sim: RDD[((String, String), Double)] = getPersonSimilarity(train.map(x=>(x._1.toString,x._2._1.toString,x._2._2)),threshold)

      val Dev = getAdjustDev(train, item_sim)
      println("***getting Recommendation")
//      val PredictSet = getRecommendation(train, Dev)
      val PredictSet = getIWSORecommendation(train, Dev, item_sim, threshold)

      val Predict3: RDD[(Int, Array[(Int, Double)])] = PredictSet.map(r => (r._1._1.toInt, Array((r._1._2.toInt, r._2)))).reduceByKey(_++_)

      println(s"****computing RMSE&MAE")
      val RM = Evaluators.rmseAndMae(Predict3, test.map(r => (r._1.toInt, r._2._1.toInt, r._2._2)))
      RMSE_s = RMSE_s + RM._1
      MAE_s = MAE_s + RM._2
      println(RM)

      time = time + 1
    }

    printf("\n***********\n ave MAE: %f\n ave RMSE: %f\n ave Pre: %f\n ave Rec: %f\n", MAE_s/time, RMSE_s/time, Pre_s/time, Rec_s/time)

    sc.stop()
  }


}
