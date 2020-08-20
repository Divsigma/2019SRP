package srp.spark

import java.util.Date

import org.apache.spark.mllib.feature.Word2Vec
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import srp.spark.utils.{DataUtils, Evaluators}

import scala.collection.immutable.IndexedSeq
import scala.collection.mutable.ListBuffer

object EWSO {

  def getVector(sc: SparkContext, data: RDD[(String, (String, Double, Long))]):
  RDD[(String, IndexedSeq[Double])] = {
    // val filePath = "hdfs://hadoop0:8020/zhongzhisong/data/ml-100k/u.data"
    val random_walk_length = 100
    val numPartitions = 5
    val numIterations = 5
    val vectorDim = 120
    val windowSize = 5

    // val walksPerVertex = 200
    val minCount = 5

    // 1.构建超图
    println("-->building graph")
    lazy val graph = HyperGraph.edgeListFile(sc, data)

    // 2.获取随机游走的序列
    println("-->obtaining walk 'context'(random walk)")
    val allEdges = graph.allEdges
    //   以每个用户的近邻用户数作为游走的步数
    lazy val walksPerVertex: Map[Int, Int] = allEdges.map(x=>(x._1,x._2.distinct.length)).collectAsMap().toMap
    //   获取随机游走的序列
    lazy val randomWalks: RDD[Array[Int]] = graph.getProbRandomWalk(random_walk_length, walksPerVertex)
    randomWalks.persist(StorageLevel.MEMORY_AND_DISK)

    // 3.序列输入W2V模型
    println("-->training w2v(deep walk)")
    val deepWalk = new Word2Vec()
      .setNumPartitions(numPartitions)
      .setNumIterations(numIterations)
      .setVectorSize(vectorDim)
      .setWindowSize(windowSize)
      .setMinCount(minCount)
    //   训练
    val model = deepWalk.fit(randomWalks.map(_.map(_.toString).toIterable))
    //   获取用户特征向量
    println("-->obtaining user vectors")
    val vectors: Map[String, Array[Float]] = model.getVectors // 核心中间变量数据

    // 4.序列化并返回用户向量RDD
    println("-->serializing user vectors")
    // 避免计算溢出(?)
    val vecRDD = sc.parallelize(vectors.toSeq.map(r => (r._1, r._2.map(ele => ele.toDouble).toIndexedSeq)))

    randomWalks.unpersist()

    vecRDD
  }

  /**
    * 返回聚类中心数组与用户分类数组的序列化对象
    * 聚类中心：[(中心id，中心向量)]
    * 用户分类：[(用户id，中心id)]
    * @param sc  配置好的SparkContext
    * @param userVec  用户特征向量
    * @param T1  Canopy阈值T1
    * @param T2  Canopy阈值T2
    * @return  (聚类中心, 用户分类) -- RDD二元组
    */
  def getCenterAndCluster(sc: SparkContext, userVec: RDD[(String, IndexedSeq[Double])], T1: Double, T2: Double):
  (RDD[(String, IndexedSeq[Double])], RDD[(String, String)]) = {

    println("-->computing initial canopy")

    val init_center = MyKMeans.getInitCenter(sc, userVec, T1, T2).collectAsMap().toArray

    println("-->clustering(Kmeans)")

    val center = MyKMeans.getCenter(
      sc,
      userVec.map(x => (x._1.toInt, x._2.toArray)),
      init_center.map(x => x._2.toArray),
      5000
    )

    val cluster = userVec.map(user => {
      val bestCenter = MyKMeans.getClosetCenter(center, (user._1.toInt, user._2.toArray))
      (user._1.toString, bestCenter._1.toString)
    })

    (sc.parallelize(center.toSeq), cluster)

  }


  def getClusterDev(clusterData: RDD[((String, String), (String, Double))]):
  RDD[((String, String, String), (Double, Int))] = {
    // 计算各聚类物品共现差值矩阵 ((cid,j,i), (devji,cji))
    //   join后：((cid,uid), ((i,ui),(j,uj))) -> ((cid,j,i),(uj-ui,1)) -> ((cid, j, i), (devji, cji))
    clusterData.join(clusterData).filter(r => r._2._2._1 != r._2._1._1)
      .map(r => ((r._1._1, r._2._2._1, r._2._1._1), (r._2._2._2 - r._2._1._2, 1)))
      .reduceByKey((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2))
      .map(r => (r._1, (r._2._1 / r._2._2, r._2._2)))
  }

  def getClusterRecommendation(clusterData: RDD[((String, String), (String, Double))], clusterDev: RDD[((String, String, String), (Double, Int))]):
  RDD[((String, String), Double)] = {
    // 用户已评分UI对
    val RatedUI = clusterData.map(r => ((r._1._1, r._1._2, r._2._1), r._2._2)) // ((cid,uid,i),ui)
    val RatedI = clusterData.map(r => ((r._1._1, r._2._1), (r._1._2, r._2._2))) // ((cid,i),(uid,ui))

    // join后为 ((cid,i),((j,devji,cji),(uid,ui)))
    val Predict = clusterDev.map(r => ((r._1._1, r._1._3), (r._1._2, r._2._1, r._2._2)))
      .join(RatedI)
      .map(r => ((r._1._1, r._2._2._1, r._2._1._1), (r._2._2._2, r._2._1._2, r._2._1._3)))
      .subtractByKey(RatedUI)
      .map(r => (r._1, (r._2._3 * (r._2._1 + r._2._2), r._2._3)))
      .reduceByKey((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2))
      .map(r => ((r._1._2, r._1._3), r._2._1 / r._2._2))

    println("complete predict")

    // val PredictTest = Predict.map(r => r._1)
    // val RatedTest = data.map(r => (r._1, r._2._1))
    //println(PredictTest.intersection(RatedTest).count())

    Predict
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("spark://202.38.236.51:7077").setAppName("EWSO")
      .set("spark.executor.memory","3g")
      .set("spark.driver.memory","10g")
      .set("spark.cores.max", "36") // 加了这种没有判错的东西。没跟别人说
      .set("spark.default.parallelism", "180")
      .set("spark.locality.wait","10")  //本地化等待时长
      .set("spark.locality.wait.node","8")  //节点内等待时长，默认3s
      .set("spark.locality.wait.rack","5")  //机架内等待时长，默认3s
      .set("spark.storage.memoryFraction","0.7")
      .set("spark.sql.shuffle.partitions","180")
      .set("spark.shuffle.consolidateFiles","true")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN") // 设置日志级别

    var time = 0
    var MAE_s = 0.0
    var RMSE_s = 0.0
    var Pre_s = 0.0
    var Rec_s = 0.0

    // 进行10次实验
    while(time < 5) {
      var start_time = new Date().getTime

      // 1、随机抽取一定数量用户，读取原始数据+游走向量
      val amount = if(args(0) != null) args(0).toInt else 600
      val cleanFilterData = sc.textFile("hdfs://202.38.236.51:8020/zhongzhisong/data/100k_data/600user/600userset/ui-600user-" + time.toString + ".dat").map(line => {
        val splits = line.replace("(", "").replace(")","").split(",")
        (splits(0), (splits(1), splits(2).toDouble, splits(3).toLong))
      })

      // 持久化
      cleanFilterData.persist()

      // 2、生成预处理后数据(读取数据->时间衰减处理->分训练测试集)
      println("***pre-treating Data")
      val TrainAndTest = DataUtils.pretreatData(cleanFilterData)
      // !!!!!!!!!注意检查顺序！
      val train = TrainAndTest._1.map(r => (r._1, (r._2._1, r._2._2)))
      val test = TrainAndTest._2.map(r => (r._1, (r._2._1, r._2._2)))
      // 持久化
      train.persist()
      test.persist()
      //    输出训练与测试数据条数
      println(train.count(), test.count(), train.intersection(test).count(), test.intersection(train).count())

      // 3、训练集上游走生成向量->聚类->计算Dev矩阵+推荐
      println("***getting user vector")
      val userVec = getVector(sc, TrainAndTest._1)
      println("***clustering(canopy+kmeans)")
      val CenterAndCluster = getCenterAndCluster(sc, userVec, args(1).toDouble, args(2).toDouble)
      val ClusterTrain = train.join(CenterAndCluster._2).map(r => ((r._2._2, r._1), r._2._1)) // ((cid,uid),(i,ui))
      println("***computing Dev on clusters")
      val ClusterDev = getClusterDev(ClusterTrain)
      println("***getting Recommendation on clusters")
      val PredictSet = getClusterRecommendation(ClusterTrain, ClusterDev)


      // 4、计算评价指标(1)
      println("****computing RMSE&MAE")
      val Predict2 = PredictSet.map(r => (r._1._1.toInt, Array((r._1._2.toInt, r._2)))).reduceByKey(_++_)
      val RM = Evaluators.rmseAndMae(Predict2, test.map(r => (r._1.toInt, r._2._1.toInt, r._2._2)))
      RMSE_s = RMSE_s + RM._1
      MAE_s = MAE_s + RM._2
      println(RM)


      // 4、计算评价指标(2)
      println("****computing Precision&Recall(Top10)")
      val Predict3 = PredictSet.map(r => (r._1._1.toInt, Array((r._1._2.toInt, r._2)))).reduceByKey(_++_)
        .map(r => (r._1, r._2.sortBy(ele => ele._2).reverse.take(10)))
      // Predict3.flatMap(r => r._2).collect().foreach(println)
      val PAndR = Evaluators.precisionAndRecall(Predict3, test.map(r => (r._1.toInt, r._2._1.toInt, r._2._2)))
      Pre_s = Pre_s + PAndR._1
      Rec_s = Rec_s + PAndR._2
      println(PAndR)
/*
      // 计算时间
      PredictSet.count()
      var end_time = new Date().getTime

      println((end_time - start_time) / 1000.0)
 */

      time = time + 1

      // 去持久化
      train.unpersist()
      test.unpersist()
      cleanFilterData.unpersist()

      printf("\n-----------------------------------\n\n")
    }

    // 5、输出实验后各评价指标平均值
    printf("\n***********\n ave MAE: %f\n ave RMSE: %f\n ave Pre: %f\n ave Rec: %f\n", MAE_s/time, RMSE_s/time, Pre_s/time, Rec_s/time)

    sc.stop()
  }
}
