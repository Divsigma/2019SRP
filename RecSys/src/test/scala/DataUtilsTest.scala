import org.apache.spark.{SparkConf, SparkContext}
import srp.spark.utils.{DataUtils, RetailCleaner}

object DataUtilsTest {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("RetailCleaner")
      .set("spark.executor.memory","3g")
      .set("spark.driver.memory","10g")
      .set("spark.cores.max", "36")
      .set("spark.default.parallelism", "180")
      .set("spark.locality.wait","10")  //本地化等待时长
      .set("spark.locality.wait.node","8")  //节点内等待时长，默认3s
      .set("spark.locality.wait.rack","5")  //机架内等待时长，默认3s
      .set("spark.storage.memoryFraction","0.7")
      .set("spark.sql.shuffle.partitions","180")
      .set("spark.shuffle.consolidateFiles","true")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN") // 设置日志级别

    DataUtils.RetailFilter(sc,
      "hdfs://202.38.236.51:8020/zhongzhisong/data/retail_data/events.csv",
      "C:\\Users\\iceloated\\Desktop\\srp\\retail_data\\ui_gt_5",
      5
    )

    DataUtils.MovielenFilter(sc,
      "C:\\Users\\iceloated\\Desktop\\srp\\ml-1m\\ratings_100k_full.data",
      "C:\\Users\\iceloated\\Desktop\\srp\\100k_data",
      300,
      10
    )

    sc.stop()

  }



}
