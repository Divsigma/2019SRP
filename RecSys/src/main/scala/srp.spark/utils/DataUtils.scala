package srp.spark.utils

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer
import scala.util.Random

object DataUtils {

  /**
    * 提取Retailrocket数据中在平台上产生事件数量大于threshold的用户，保存到指定位置。
    * 数据形式：
    *   userId,itemId,score,timestamp
    * 事件被人为给予score：
    *   “view” -- 1分
    *   “addtocart” -- 2分
    *   “transaction” -- 3分
    * @param sc  配置好的SparkContext
    * @param rawPath  文件源地址，如"hdfs://202.38.236.51:8020/zhongzhisong/data/retail_data/events.csv"
    * @param basePath  文件目标地址根目录，如"C:\\Users\\iceloated\\Desktop\\srp\\retail_data"
    *                  文件会以名称：
    *                  <basePath>/rr-gt-<threshold>保存
    * @param threshold  产生事件数量的最小值，如5
    */
  def RetailFilter(sc: SparkContext, rawPath: String, basePath: String, threshold: Int): Unit = {

    val rawData = sc.textFile(rawPath).map(line => {
      val splits = line.split(",")
      splits(2) match {
        case "view" => (splits(1), (splits(3), 1, splits(0).toLong))
        case "addtocart" => (splits(1), (splits(3), 2, splits(0).toLong))
        case "transaction" => (splits(1), (splits(3), 3, splits(0).toLong))
      }
    })

    val data = rawData.map(r => ((r._1, r._2._1, r._2._2), r._2._3)).reduceByKey((v1, v2) => if(v1 > v2) v1 else v2)
      .map(r => ((r._1._1, r._1._2), ListBuffer((r._1._3, r._2)))).reduceByKey(_++_)
      .map(r => (r._1, r._2.maxBy(ele => ele._1))).map(r => (r._1._1, (r._1._2, r._2._1, r._2._2)))

    val len = data.map(r => (r._1, 1)).reduceByKey(_+_).collectAsMap()
    val shuffleData = data.map(r => (r._1, ListBuffer( r ))).reduceByKey(_++_).filter(r => len(r._1) > threshold).flatMap(r => r._2)

    shuffleData.repartition(1).saveAsTextFile(basePath + "/" + "rr-gt-" + threshold.toString)

  }


  /**
    * 生成total份数据保存到指定位置。每份数据随机抽取了userAmount个用户的记录
    * @param sc  配置好的SparkContext
    * @param rawPath  文件源地址，如"C:\\Users\\iceloated\\Desktop\\srp\\ml-1m\\ratings_100k_full.data"
    * @param basePath  文件目标地址的根目录，如"C:\\Users\\iceloated\\Desktop\\srp\\100k_data\\"
    *                  文件会以名称：
    *                  <basePath>/<userAmount>-<number of this file>"保存，
    *                  其中<number of this file>按照1到total按顺序编号
    * @param userAmount  每份文件中用户的数量，如300
    * @param total  生成的文件数量，根据实验10折/5折确定，此处取10
    */
  def MovielenFilter(sc: SparkContext, rawPath: String, basePath: String, userAmount: Int, total: Int = 10): Unit = {

    val data = sc.textFile(rawPath).map(line => {
      val splits = line.split("\\s+")
      (splits(0), (splits(1), splits(2), splits(3).toLong))
    })

    val seq = data.map(r => (r._1, 1)).distinct().collectAsMap().toList

    var times = 0
    while (times < total) {
      val UserList = Random.shuffle(seq).take(userAmount).map(ele => ele._1)

      val filterData = data.filter(r => UserList.contains(r._1))

      filterData.repartition(1).saveAsTextFile(basePath + "/" + userAmount.toString + "-" + times.toString)

      times = times + 1
    }

  }


  /**
    * 对RDD型数据进行预处理
    *   归一化 + 时间衰减
    * @param rawData  RDD形式的原始数据
    * @return  (训练数据，测试数据) -- RDD二元组
    */
  def pretreatData(rawData: RDD[(String, (String, Double, Long))]):
  (RDD[(String, (String, Double, Long))], RDD[(String, (String, Double, Long))]) = {

    // 1.读取数据(并过滤出随机抽取的用户)
    println("->reading data")

    val minmaxData = rawData.map(r => (r._1, (r._2._1, r._2._2, r._2._3)))
    //   归一化处理(分母可能为0)
    val userMaxMin = minmaxData.map(r => (r._1, ListBuffer(r._2._2))).reduceByKey(_++_).map(r => (r._1, (r._2.max, r._2.min)))
    val data = minmaxData.join(userMaxMin).map(r => (r._1, (r._2._1._1, (r._2._1._2 - r._2._2._2) / (r._2._2._1 - r._2._2._2), r._2._1._3)))

    // 2、按比例划分数据集，要求数据按时间先后排序(即差值大的/时间戳小的在前)后再划分
    // 不排序基于shuffleData划分的数据集可能会有交集(又是map的lazy evaluation...)
    println("->splitting data(8:2)")

    val len = data.map(r => (r._1, 1)).reduceByKey(_+_).collectAsMap()
    val shuffleData = data.map(r => (r._1, ListBuffer( r ))).reduceByKey(_++_).map(r => (r._1, r._2.sortBy(ele => ele._2._3)))

    // 注意要切断联系
    shuffleData.persist()
    println(shuffleData.flatMap(r => r._2).count())

    val t0 = data.map(r => r._2._3).max() // 获取基准时间点

    val preTrain = shuffleData.flatMap(r => r._2.take(8 * len(r._1) / 10))
    val preTest = shuffleData.flatMap(r => r._2).subtract(preTrain)

    // 3.时间衰减处理
    println("->time decay treatment")

    val train = preTrain.map(r => (r._1, (r._2._1, r._2._2 * (1 - (2 / math.Pi) * math.atan(math.abs(t0 - r._2._3) / 86400000.0)), r._2._3)))
    val test = preTest.map(r => (r._1, (r._2._1, r._2._2 * (1 - (2 / math.Pi) * math.atan(math.abs(t0 - r._2._3) / 86400000.0)), r._2._3)))
    //    输出训练与测试数据条数
    // println(preTrain.count(), preTest.count(), preTrain.intersection(preTest).count(), preTest.intersection(preTrain).count())

    //  4、返回数据
    (train, test)
  }



}
