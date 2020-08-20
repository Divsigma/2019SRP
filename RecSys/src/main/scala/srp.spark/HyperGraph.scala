package srp.spark

import org.apache.spark.sql._
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.storage.StorageLevel

import scala.util.Random
import org.apache.spark.rdd.RDD


class HyperGraph(edges:RDD[(Int, Int, Double)]) {

  // 用户近邻列表(不包括相似度边权)，用来进行随机游走
  val adjacencyList: RDD[(Int, Array[Int])] = edges.map(x=>(x._1, List(x._2)))
    .reduceByKey(_ ++ _)
    .mapValues(_.toArray)
    .partitionBy(new HashPartitioner(30))
    .persist(StorageLevel.MEMORY_AND_DISK)

  // 用户近邻列表(包括相似度边权)，用来进行概率游走
  val allEdges: RDD[(Int, Array[(Int, Double)])] = edges
//    .union(edges.map(x=>(x._2,x._1,x._3)))
    .map(x=>(x._1,List((x._2,x._3))))
    .reduceByKey(_ ++ _)
    .mapValues(_.toArray)
    .partitionBy(new HashPartitioner(30)) // 改为30分区
    .persist(StorageLevel.MEMORY_AND_DISK)


  def getRandomWalks(walkLength: Int,walksPerVertex: Map[Int, Int]): RDD[Array[Int]] = {
    // Bootstrap the random walk from every vertex
    var keyedRandomWalks = adjacencyList.keys.flatMap(id => {
      for (iter <- 1 to walksPerVertex(id))
        yield {
          val walk = new Array[Int](walkLength)
          walk(0) = id
          (id, walk)
        }
    })

    // Grow the walk choosing a random neighbour uniformly at random
    for (iter <- 1 until walkLength) {
      val grownRandomWalks =
        adjacencyList.join(keyedRandomWalks)
          .map {
            case (node_id, (neighbours, walk)) =>
              val r = new Random()
              val randomNeighbour = neighbours(r.nextInt(neighbours.length))  // 可能需要-1
              walk(iter) = randomNeighbour
              (randomNeighbour, walk )

          }

      keyedRandomWalks.unpersist()
      keyedRandomWalks = grownRandomWalks

    }

    keyedRandomWalks.values
  }

  // 概率游走
  def getProbRandomWalk(walkLength: Int, walksPerVertex: Map[Int, Int]): RDD[Array[Int]] = {

    val transEdges: RDD[(Int, Array[(Int, (Double, Double))])] = allEdges.map{
      x=>
        val len = x._2.length  // 近邻用户个数
        val maxValue = x._2.maxBy(t=>t._2)._2
        val ratioList = x._2.map(t=>(t._1,t._2.toDouble / maxValue.toDouble))  // 计算每个近邻用户边权的比例(user,ratioWeight)
        val tmpList = ratioList.sortBy(_._2) // 按照边权大小升序排列
        val pos: Array[(Double, Double)] = new Array[(Double,Double)](len)  // 区间变量数组

        // 获取每个边权所属的区间
        for(i <- 0 until len){
          if (i == 0)
            pos(i) = (0.0, tmpList(i)._2)
          else
            pos(i) = (tmpList(i - 1)._2, tmpList(i)._2)
        }

        val finaList: Array[(Int, (Double, Double))] = tmpList.zip(pos).map(x=>(x._1._1,x._2))  // 获取每个近邻用户所在的权重区间
        (x._1,finaList)
    }

    // 每个节点规定游走walksPerVertex次
    var keyedRandomWalks: RDD[(Int, Array[Int])] = transEdges.keys.flatMap(id=>{   // 报错
      for (iter <- 1 to walksPerVertex(id))
//      for (iter <- 1 to 100)
        yield{
          val walks = new Array[Int](walkLength)
          walks(0) = id
          (id,walks)
        }
    })


    for(i <- 1 until walkLength){
      val growsRandomWalks: RDD[(Int, Array[Int])] = transEdges.join(keyedRandomWalks).map{
        case (node_id, (neighbors, walks))=>
          val r = new Random().nextDouble()  // 生成0-1 的随机数
          val randomNeighbour = neighbors(neighbors.indexWhere(x=> r > x._2._1 && r <= x._2._2))//(数组越界) 将包含该随机数值的区间对应的节点取出
          walks(i) = randomNeighbour._1
          (randomNeighbour._1, walks)
      }
      keyedRandomWalks.unpersist()
      keyedRandomWalks = growsRandomWalks

    }

    keyedRandomWalks.values

  }

  adjacencyList.unpersist()
  allEdges.unpersist()

}

object HyperGraph extends Serializable {

  // 处理(3)：原始数据，衰减，无mean
  //         用公式(1- (2 / Pi) * arctan(t0 - ti))进行时间衰减，不进行like和dislike 划分
  def dealData3(sc: SparkContext, data: RDD[(String, (String, Double, Long))]) = {

    val rawData = data.map(r => (r._1.toInt, r._2._1.toInt, r._2._2, r._2._3))

    // 获取基准时间点
    // val nowTime = rawData.map(x=>x._4).max()

    // 先进行时间衰减，再进行min-max 规范化，【数据传入前已处理】
    // (item,(user, item, score, time))
    val transScore: RDD[(Int, (Int, Int, Double, Long))] = rawData.keyBy(_._2)

    // 用户个人评分之和
    val user_sum_score = transScore.map(x=>(x._2._1,x._2._3)).reduceByKey(_ + _).collectAsMap()

    // 获取用户共现项目的评分，计算用户间的相似度
    /*
    val common_user_pairs = transScore.join(transScore).filter(x=>x._2._1._1 != x._2._2._1).map(x=>{
      val score = ((x._2._1._3 + x._2._2._3) / 2) * (1 - 2 / (1 + math.exp(math.abs(x._2._1._4 - x._2._2._4) / 86400.0)))
      ((x._2._1._1,x._2._2._1),score / math.sqrt(user_sum_score(x._2._1._1) * user_sum_score(x._2._2._1)))
    }).reduceByKey(_ + _).map(x=>(x._1._1,x._1._2,x._2))*/

    val common_user_pairs = transScore.join(transScore).filter(x=>x._2._1._1 != x._2._2._1).map(x=>{
      val score = (x._2._1._3 + x._2._2._3) / 2
      ((x._2._1._1,x._2._2._1),score / math.sqrt(user_sum_score(x._2._1._1) * user_sum_score(x._2._2._1)))
    }).reduceByKey(_ + _).map(x=>(x._1._1,x._1._2,x._2))

    common_user_pairs

  }

  def edgeListFile(sc:SparkContext, data: RDD[(String, (String, Double, Long))]) = {

    val edges: RDD[(Int, Int, Double)] = dealData3(sc, data)

    new HyperGraph(edges)
  }

}
