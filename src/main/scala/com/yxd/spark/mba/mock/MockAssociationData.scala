package com.yxd.spark.mba.mock

import java.util.concurrent.ThreadLocalRandom

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer


/**
  * 产生模拟数据
  *
  */
object MockAssociationData {
  // 随机数产生器
  val random = ThreadLocalRandom.current()
  // 数据分割符号
  val delimiter = ","
  // 商品类型，总共26类
  val goodTypes = "qwertyuioplkjhgfdsazxcvbnm"
    .map(ty => s"${ty}_".toUpperCase)
    // 减少部分商品类型数量
    .zipWithIndex.filter(_._2 % 5 == 0).map(_._1)
  // 商品类型数量
  val goodTypeSize = goodTypes.size
  // 每种类型商品的数量
  val goodSizes = goodTypes.map(ty => {
    // 每个类型的商品的最少数量及最多数量
    val min = goodTypeSize * 5
    val max = goodTypeSize * 10
    random.nextInt(min, max)
  })
  // 每条交易的最少商品数和最多商品数[1,10], 最大可能的商品数区间[2,5]
  val minGoodNumPreTransaction = 1
  val mostLikelyMinGoodNumPreThransaction = 2
  val mostLikelyMaxGoodNumPreThransaction = 5
  val maxGoodNumPreTransaction = 10
  // 80%的商品件数在[2,5]件
  val mostLikelyRatePreThransaction = 0.8
  // 每条交易记录中，最多商品类型占比；这个数据越大，表示一条交易中的绝大多数商品类型不一致
  val maxGoodTypeRate = 0.7

  /**
    * 产生一个模拟的交易数据，返回一个RDD对象，RDD对象中的数据就是交易数据<br/>
    * 每条交易数据包含了所有的该条交易的所有商品
    *
    * @param sc
    * @param n          需要产生N条记录
    * @param partitions RDD的分区数量
    * @return
    */
  def mockTransactionData(sc: SparkContext, n: Int, partitions: Int): RDD[String] = {
    // 随机N个交易信息
    val transactions = (0 until n).map(n => {
      // 获取该条交易绝大多数商品所属类型，比如：电器、商品之类的
      val index = random.nextInt(goodTypeSize)
      val goodType = goodTypes(index)
      // 随机一个当前交易的商品数量，最少1件商品，最多19件
      val transactionGoodSize = if (random.nextDouble(1) > mostLikelyRatePreThransaction) {
        // 产生比较少的商品数或者比较多的商品数
        if (random.nextBoolean()) {
          // 产生比较多的商品数
          random.nextInt(mostLikelyMaxGoodNumPreThransaction + 1, maxGoodNumPreTransaction + 1)
        } else {
          // 产生比较少的商品数
          random.nextInt(minGoodNumPreTransaction, mostLikelyMinGoodNumPreThransaction)
        }
      } else {
        // 产生正常商品数量
        random.nextInt(mostLikelyMinGoodNumPreThransaction, mostLikelyMaxGoodNumPreThransaction + 1)
      }

      // 随机商品
      val goodBuffer = ArrayBuffer[(String, Int)]()
      (0 until transactionGoodSize).foreach(s => {
        // 有百分之十的几率商品不是goodType类型的
        val (gt, gi) = if (random.nextDouble(1) > maxGoodTypeRate) {
          // 产生的是未知类型的商品
          val gi = random.nextInt(goodTypeSize)
          (goodTypes(gi), gi)
        } else {
          (goodType, index)
        }

        // 随机一个商品
        var good = (gt, random.nextInt(goodSizes(gi)))
        while (goodBuffer.contains(good)) {
          // 由于产生的商品重复，所以重新产生一个
          good = (gt, random.nextInt(goodSizes(gi)))
        }
        // 将产生的商品添加到buffer中
        goodBuffer += good
      })

      // 随机的商品格式化
      goodBuffer.map(tuple => "%s%04d".format(tuple._1, tuple._2)).mkString(delimiter)
    })

    // 构建RDD并返回
    sc.parallelize(transactions, partitions)
  }

  /**
    * 产生N条模拟数据，并输出到指定目录文件夹中
    *
    * @param sc
    * @param n
    * @param path
    */
  def mockTransactionDataAndSaveToPath(sc: SparkContext, n: Int, path: String, partitions: Int) = {
    // 产生模拟数据
    val rdd = mockTransactionData(sc, n, partitions)
    // 模拟数据输出
    rdd.saveAsTextFile(path)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("test")
    val sc = SparkContext.getOrCreate(conf)

    val n = 10000
    val path = s"data/transactions/${n}"
    val partitions = 1
    // 如果目录存在，删除之
    FileSystem.get(sc.hadoopConfiguration).delete(new Path(path), true)
    mockTransactionDataAndSaveToPath(sc, n, path, partitions)

    sc.stop()
  }
}
