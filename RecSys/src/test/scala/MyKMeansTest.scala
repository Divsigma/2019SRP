import org.apache.spark.{SparkConf, SparkContext}

import srp.spark.MyKMeans

object MyKMeansTest {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName("MyKMeansTest")
      .setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    val userRDD = sc.parallelize(Seq(
      (1, Array(2.0, 10.0)),
      (2, Array(2.0, 5.0)),
      (3, Array(8.0, 4.0)),
      (4, Array(5.0, 8.0)),
      (5, Array(7.0, 5.0)),
      (6, Array(6.0, 4.0)),
      (7, Array(1.0, 2.0)),
      (8, Array(4.0, 9.0))
    ))

    val initCenter = Array(
      Array(2.0, 10.0),
      Array(5.0, 8.0),
      Array(1.0, 2.0)
    )

    /*
        val userRDD = sc.parallelize(Seq(
          (1, Array(0.0, 0.0)),
          (2, Array(1.0, 2.0)),
          (3, Array(3.0, 1.0)),
          (4, Array(8.0, 8.0)),
          (5, Array(9.0, 10.0)),
          (6, Array(10.0, 7.0))
        ))

        val initCenter = Array(
          Array(0.0, 0.0),
          Array(1.0, 2.0)
        )

     */

    val res = MyKMeans.getCenterForTest(sc, userRDD, initCenter, 100)
    res.indices.foreach(i => {
      print("center " + i + " : ")
      res(i).foreach(x => print(x + " "))
      print("\n")
    })

  }

}
