package srp.spark

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.immutable.IndexedSeq
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object MyKMeans {

  def calDistance(x: Array[Double], y: Array[Double]): Double = {

    var dis: Double = 0.0

    for(i <- x.indices) {
      dis += (x(i)-y(i)) * (x(i)-y(i))
    }

    math.sqrt(dis)

  }

  def calSum(x: Array[Double], y: Array[Double]): Array[Double] = {

    var sum = Array.fill(x.length)(0.0)

    for(i <- x.indices) {
      sum(i) = x(i) + y(i)
    }

    sum

  }

  def getClosetCenter(centerBcValue: Array[Array[Double]], user: (Int, Array[Double])):
  (Int, Double) = {

    var closetDistance = Double.PositiveInfinity
    var bestId = -1

    centerBcValue.indices.foreach(cId => {
      val distance = calDistance(centerBcValue(cId), user._2)
      if(distance < closetDistance) {
        closetDistance = distance
        bestId = cId
      }
    })

    (bestId, closetDistance)

  }

  // KMeans for test
  def getCenter(sc: SparkContext, userRDD: RDD[(Int, Array[Double])], initCenter: Array[Array[Double]], maxIterations: Int):
  Array[Array[Double]] = {

    var preCost = 0.0
    var curCost = 0.0
    var iteration = 0
    var converged = false

    var center = initCenter
    val centerNum = initCenter.length
    val dimension = initCenter.head.length

    while(iteration < maxIterations && !converged) {

      val costAcc = sc.doubleAccumulator("costAccumulator")
      val centerBc = sc.broadcast(center)

      // Get new center -- step 1
      // calculate sum of vector and #user in center by partitions and
      // aggregate them by reduceByKey() and collectAsMap()
      val newCenterSumAndCount = userRDD.mapPartitions(rdd => {

        val centerBcValue = centerBc.value
        val sums = Array.fill(centerNum)(Array.fill(dimension)(0.0))
        val counts = Array.fill(centerNum)(0l)

        rdd.foreach(user => {

          val (cId, cost) = getClosetCenter(centerBcValue, user)
          // Accumulate cost, center_vector_sum and center_count
          costAcc.add(cost)
          counts(cId) += 1
          sums(cId) = calSum(sums(cId), user._2)

        })

        counts.indices.filter(counts(_) > 0).map(i => (i, (sums(i), counts(i)))).iterator

      }).reduceByKey((v1, v2) => (calSum(v1._1, v2._1), v1._2 + v2._2)).collectAsMap()

      // Get new center -- step 2
      // calculate new center: sums/counts and replace old center with it
      // remember to destroy the broadcast variable (it is cached in memory)
      val newCenterMap = newCenterSumAndCount.mapValues(x => {
        val agg = Array.fill(x._1.length)(0.0)
        for(i <- x._1.indices) {
          agg(i) = x._1(i) / x._2.toDouble
        }
        agg
      })

      centerBc.destroy()

      newCenterMap.foreach{ case (cId, newCenter) => center(cId) = newCenter }

      preCost = curCost
      curCost = costAcc.value
      iteration += 1

      println("iteration: " + iteration + " ---- cost: " + curCost)

      if(math.abs(preCost-curCost) < 1e-5) {
        converged = true
      }

    }

    // Return cluster center Array
    center

  }

  // Canopy
  def getInitCenter(sc: SparkContext, user: RDD[(String, IndexedSeq[Double])], T1: Double, T2: Double): RDD[(String, IndexedSeq[Double])] = {
    val point_array = user.map(r => r._1).collect().toBuffer // ArrayBuffer -> [u1, u2, ...]
    val canopy_list = ListBuffer[(String, String)]() // [(c1, uid1), (c1, uid2), ..., (c3, uid1), ...]
    var cid = 0
    while(point_array.nonEmpty) {
      // 1.选择未归类或未作为中心的点序列中的一个点p，以p初始化一个新的聚类中心
      val p = point_array.head // 取点
      point_array.remove(0) // 从集合中移除
      cid = cid + 1
      canopy_list += ( (cid.toString, p) )

      // 2.并行计算距离并分类
      val p_vec = user.lookup(p).head
      val user_left = user.filter(u => point_array.contains(u._1))
      // 计算距离并过滤出与p小于T1的点
      val res = user_left.map(u => (u._1, for(i <- u._2.indices) yield math.pow(u._2(i) - p_vec(i), 2)))
        .map(u => (u._1, math.sqrt(u._2.sum))).filter(_._2 <= T1).cache()
      // 小于T1的点加入新聚类
      res.collect().toList.foreach(u => canopy_list += ( (cid.toString, u._1) ))
      // 小于T2的点移出point_array
      res.filter(_._2 <= T2).collect().toList.foreach(u => point_array -= u._1)
      res.unpersist()
    }

    // join后为(uid, (cid, [u_vec])) -> (cid, ([u_vec], 1)) -> (cid, [c_vec])
    sc.parallelize(canopy_list).map(c => (c._2, c._1)).join(user).map(r => (r._2._1, (r._2._2, 1)))
      .reduceByKey((v1, v2) => (for(i <- v1._1.indices) yield v1._1(i) + v2._1(i), v1._2 + v2._2))
      .map(c => (c._1, for(i <- c._2._1.indices) yield c._2._1(i) / c._2._2))

  }




  // KMeans for test
  def getCenterForTest(sc: SparkContext, userRDD: RDD[(Int, Array[Double])], initCenter: Array[Array[Double]], maxIterations: Int):
  Array[Array[Double]] = {

    var preCost = 0.0
    var curCost = 0.0
    var iteration = 0
    var converged = false

    var center = initCenter
    val centerNum = initCenter.length
    val dimension = initCenter.head.length

    while(iteration < maxIterations && !converged) {

      val costAcc = sc.doubleAccumulator("costAccumulator")
      val centerBc = sc.broadcast(center)

      // Get new center -- step 1
      // calculate sum of vector and #user in center by partitions and
      // aggregate them by reduceByKey() and collectAsMap()
      val newCenterSumAndCount = userRDD.mapPartitions(rdd => {

        val centerBcValue = centerBc.value
        val sums = Array.fill(centerNum)(Array.fill(dimension)(0.0))
        val counts = Array.fill(centerNum)(0l)

        rdd.foreach(user => {

          val (cId, cost) = getClosetCenter(centerBcValue, user)
          // Accumulate cost, center_vector_sum and center_count
          costAcc.add(cost)
          counts(cId) += 1
          sums(cId) = calSum(sums(cId), user._2)

        })

        counts.indices.filter(counts(_) > 0).map(i => (i, (sums(i), counts(i)))).iterator

      }).reduceByKey((v1, v2) => (calSum(v1._1, v2._1), v1._2 + v2._2)).collectAsMap()

      // Get new center -- step 2
      // calculate new center: sums/counts and replace old center with it
      // remember to destroy the broadcast variable (it is cached in memory)
      //
      // BUG: be careful of using mapValue()
      // val newCenterMap = newCenterSumAndCount.map(x => {
      // for(i <- x._2._1.indices) {
      //   x._2._1(i) /= x._2._2.toDouble
      // }
      // (x._1, x._2._1)
      // })
      val newCenterMap = newCenterSumAndCount.mapValues(x => {
        println(" -- call mapValues() --")
        val agg = Array.fill(x._1.length)(0.0)
        for(i <- x._1.indices) {
          agg(i) = x._1(i) / x._2.toDouble
          //x._1(i) /= x._2.toDouble
        }
        //x._1
        agg
      })

      println("before destroy")
      newCenterMap.foreach(x => {
        print(x._1 + " : ")
        x._2.foreach(y => print(y + " "))
        println()
      })
      println("========================")

      centerBc.destroy()

      println("after destroy")
      newCenterMap.foreach(x => {
        print(x._1 + " : ")
        x._2.foreach(y => print(y + " "))
        println()
      })
      println("========================")

      newCenterMap.foreach{ case (cId, newCenter) => center(cId) = newCenter }

      println("************************")
      println("after assignment")
      center.indices.foreach(x => {
        print(x + " : ")
        center(x).foreach(y => print(y + " "))
        println()
      })
      println("========================")

      println("after assignment 1")
      center.indices.foreach(x => {
        print(x + " : ")
        center(x).foreach(y => print(y + " "))
        println()
      })
      println("========================")

      preCost = curCost
      curCost = costAcc.value
      iteration += 1

      println("iteration: " + iteration + " ---- cost: " + curCost)

      if(math.abs(preCost-curCost) < 1e-5) {
        converged = true
      }

    }

    // Return Array cluster center
    center

  }

}
