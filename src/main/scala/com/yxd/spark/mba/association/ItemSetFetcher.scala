package com.yxd.spark.mba.association

import scala.collection.mutable

/**
  * 项集的获取
  * Created by ibf on 01/18.
  */
object ItemSetFetcher {
  def main(args: Array[String]): Unit = {
    val transaction = "A,B,C,D,E,F"
    // 交易数据格式化
    val items = transaction.split(",").toList.sorted.zipWithIndex
    // 创建cache
    val cache = mutable.Map[Int, List[List[(String, Int)]]]()
    // 获取大小为1的数据集合
    println("***************")
    findItemSetsByCache(items, 1, cache).foreach(println)
    // 获取大小为2的数据集合
    println("***************")
    findItemSetsByCache(items, 2, cache).foreach(println)
    // 获取大小为3的数据集合
    println("***************")
    findItemSetsByCache(items, 3, cache).foreach(println)
    // 获取大小为4的数据集合
    println("***************")
    findItemSetsByCache(items, 4, cache).foreach(println)
    // 获取大小为5的数据集合
    println("***************")
    findItemSetsByCache(items, 5, cache).foreach(println)
  }

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
}
