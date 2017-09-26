package com.yxd.spark.mba.association

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * 使用SparkCore实现购物篮分析
  * Created by ibf on 01/12.
  */
object FindAssociationRulesSparkCore {
  /**
    * 先从缓存中获取数据，如果不存在，直接重新获取
    *
    * @param items
    * @param size
    * @param cache
    * @return
    */
  def findItemSetsByCache(items: List[(String, Int)], size: Int, cache: mutable.Map[Int, List[List[(String, Int)]]]): List[List[(String, Int)]] = {
    cache.get(size).orElse {
      // 获取值
      val result = findItemSets(items, size, cache)

      // 更新缓存
      cache += size -> result

      // 返回值
      Some(result)
    }.get
  }


  /**
    * 构建项集基于items商品列表，项集中的商品数量是size指定
    *
    * @param items 商品列表：eg: [A, B, C]
    * @param size  最终项集包含商品的数量
    * @return
    */
  def findItemSets(items: List[(String, Int)], size: Int, cache: mutable.Map[Int, List[List[(String, Int)]]]): List[List[(String, Int)]] = {
    if (size == 1) {
      // items中的每个商品都是一个项集
      items.map(item => item :: Nil)
    } else {
      // 当size不是1的时候
      // 1. 获取项集大小为size-1的项集列表
      val tmpItemSets = findItemSetsByCache(items, size - 1, cache)
      // 2. 给tmpItemSets中添加一个新的不重复的项 ==> 数据的转换
      val itemSets = tmpItemSets.flatMap(itemSets => {
        // 给itemSets项集添加一个新的商品ID，要求不重复
        val newItemSets = items
          // 将包含的商品过滤掉&要求下标必须大于以及存在
          .filter(item => !itemSets.contains(item) && itemSets.forall(_._2 < item._2))
          // 将商品添加到项集中，产生一个新的项集
          // 为了使用distinct做去重操作，进行一个排序操作
          .map(item => (item :: itemSets))

        // 返回值
        newItemSets
      })

      // 返回项集的值
      itemSets
    }
  }

  def main(args: Array[String]): Unit = {
    val n = 10000
    // 1. 创建SparkContext
    val conf = new SparkConf()
      .setAppName(s"find-association-rules-${n}")
      .setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)

    // ===========================================
    // 测试数据存储的路径
    val path = s"data/transactions/${n}"
    val savePath = s"result2/${n}"
    // 最小支持度
    val minSupport = 2
    // 最小置信度
    val minConfidence = 0.1

    // 创建rdd读取原始的交易数据，
    // 假设交易数据是按行存储的，每行是一条交易，每条交易数据包含的商品ID使用","分割
    val rdd = sc.textFile(path, 20)

    // 过滤无效数据：对于在整个交易集合中出现比较少的商品过滤掉，先进行需要过滤的商品的RDD数据
    val minGoodCount = 3 // 要求商品在整个交易集中至少出现3次
    val needFilterGoodsRDD = rdd
      .flatMap(transaction =>
        transaction
          .split(",")
          .filter(!_.isEmpty)
          .map(good => (good, 1))
      )
      .reduceByKey(_ + _)
      .filter(_._2 < minGoodCount)
      .map(_._1)
    // 使用广播变量将数据广播输出
    val needFilterGoods: Broadcast[List[String]] = sc.broadcast(needFilterGoodsRDD.collect().toList)

    // 1. 计算频繁项集
    // 1.1 获取每条交易存在的项集
    val itemSetsRDD: RDD[String] = rdd.flatMap(transaction => {
      // 1) 获取当前交易所包含的商品ID
      val goods: Array[String] = transaction
        .split(",") // 分割
        .filter(!_.isEmpty) // 过滤


      // 将需要过滤的数据过滤掉
      val items = (goods.toBuffer -- needFilterGoods.value)
        .sorted //排序
        .toList // 转换为list
        .zipWithIndex // 将数据和下标合并，下标从0开始

      // 2) 构建辅助对象
      // 最大的项集只允许存在5个项的，5怎么来？根据业务规则&根据运行之后的情况
      val itemSize = Math.min(items.size, 5)
      val cache = mutable.Map[Int, List[List[(String, Int)]]]()

      // 3) 根据获取的商品ID的信息产生项集
      // allItemSets集合中最后数据量是:2^itemSize - 1
      val allItemSets: List[List[String]] = (1 to itemSize).map(size => {
        // 产生项集中项的数量是size的项集
        findItemSets(items, size, cache)
      }).foldLeft(List[List[String]]())((v1, v2) => {
        v2.map(_.map(_._1)) ::: v1
      })

      // 4) 返回结果
      allItemSets.map(_.mkString(","))
    })

    // 1.2 获取频繁项集
    val supportedItemSetsRDD = itemSetsRDD
      // 数据转换
      .map(items => (items, 1))
      // 聚合求支持度
      .reduceByKey(_ + _)
      // 过滤产生频繁项集
      .filter(_._2 >= minSupport)

    // 2. 计算关联规则
    // 2.1 对每个频繁项集获取子项集
    val subSupportedItemSetsRDD = supportedItemSetsRDD.flatMap(tuple => {
      val itemSets = tuple._1.split(",").toList.zipWithIndex // 频繁项集
      val frequency = tuple._2 // 该频繁项集的支持度

      // 2) 构建辅助对象
      val itemSize = itemSets.size
      val cache = mutable.Map[Int, List[List[(String, Int)]]]()

      // 3) 获取子项集
      val allSubItemSets: List[List[String]] = (1 to itemSize).map(size => {
        // 产生项集中项的数量是size的项集
        findItemSets(itemSets, size, cache)
      }).foldLeft(List[List[String]]())((v1, v2) => {
        v2.map(_.map(_._1)) ::: v1
      })

      // 4) 转换数据并输出
      val items = itemSets.map(_._1)
      allSubItemSets.map(subItemSets => {
        // (A,B,frequency) ==> 表示A出现的时候B也出现的次数是frequency次
        // 当subItemSets就是itemSets的时候，返回的二元组的第二个元素的(元组)第一个元素是空的列表
        (subItemSets.mkString(","), ((items.toBuffer -- subItemSets).toList.mkString(","), frequency))
      })
    })

    // 2.2 计算置信度
    val assocRulesRDD = subSupportedItemSetsRDD
      .groupByKey() // 数据聚合
      .flatMap(tuple => {
      // 计算执行度: (A, B, k) => A存在的时候B也存储的几率是k
      // A就是tuple的第一个元素
      // 获取左件
      val lhs = tuple._1.split(",").mkString("<", ",", ">")

      // 获取左件在所有的交易中出现的总的次数 tuple._2中第一个元素为空的数据就是总的次数
      val frequency = tuple._2
        // 只要第一个元素为空的值，表示from本身
        .filter(_._1.isEmpty)
        // 需要的是第二个元素
        .map(_._2).toList match {
        case head :: Nil => head
        case _ => {
          throw new IllegalArgumentException("异常")
        }
      }

      // 计算右件出现次数占左件次数的百分比, 并返回最终结果
      tuple._2
        // 要求第一个数据非空
        .filter(!_._1.isEmpty)
        // 数据转换，获取置信度
        .map {
        case (rhs, support) => {
          // 计算置信度
          (lhs, rhs.split(",").mkString("<", ",", ">"), 1.0 * support / frequency)
        }
      }
    })

    // 2.3 过滤置信度太低的数据
    val resultRDD = assocRulesRDD.filter(_._3 >= minConfidence)

    // 3. RDD数据保存
    FileSystem.get(sc.hadoopConfiguration).delete(new Path(savePath), true)
    resultRDD.repartition(1).saveAsTextFile(savePath)

    // ===========================================
    sc.stop()
  }
}
