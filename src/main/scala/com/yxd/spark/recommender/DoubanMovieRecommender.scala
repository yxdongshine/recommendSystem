package com.yxd.spark.recommender

import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * git 证书问题：http://blog.csdn.net/sdhongjun/article/details/52144253
 */
object DoubanMovieRecommender {
  def main(args: Array[String]): Unit = {
    // 一、创建上下文
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("DoubanMovieRecommender")
    val sc = SparkContext.getOrCreate(conf)

    // 二、读取数据
    val rawUserMoviesDataPath = "data/user_movies_small.csv"
    val rawUserMoviesDataRDD = sc.textFile(rawUserMoviesDataPath)

    // 三、数据转换操作
    val (userIDStats, formattedUserMoviesRDD) = this.preparation(rawUserMoviesDataRDD, sc)
    userIDStats.cache()

    // 四、模型的构建
    val ratings = formattedUserMoviesRDD.map {
      case (userID, movieID, rating) => {
        Rating(userID, movieID, rating)
      }
    }
    ratings.cache()
    val t0 = System.currentTimeMillis()
    val model = ALS.train(ratings, 10, 20, 0.001)
    println(s"模型构建完成，消耗时间:${System.currentTimeMillis() - t0}")

    // 模型进行一下打印操作
    println("第一个商品的特征属性:" + model.productFeatures.mapValues(_.mkString(", ")).first())
    println("第一个用户的特征属性:" + model.userFeatures.mapValues(_.mkString(", ")).first())

    // 做推荐预测
    val userID2userNameCol = userIDStats.collect().map(_.swap).toMap
    val num = 3
    // 基于用户id做推荐预测
    for (userID <- Array(1, 10, 100, 1000, 10000)) {
      val result = model.recommendProducts(userID, num)
      println(s"用户ID=${userID}的用户获取推荐列表================")
      result
        .map(rating => {
          (userID2userNameCol.getOrElse(rating.user, rating.user), rating.product, rating.rating)
        })
        .foreach(println)
      println("\n\n")
    }
    // 基于物品做推荐
    for (productID <- Array(20645098, 1866473)) {
      val result = model.recommendUsers(productID, num)
      println(s"物品ID=${productID}的物品获取推荐列表================")
      result
        .map(rating => {
          (userID2userNameCol.getOrElse(rating.user, rating.user), rating.product, rating.rating)
        })
        .foreach(println)
      println("\n\n")
    }

    // 为了看4040页面:
    Thread.sleep(1000000)
  }

  def preparation(userMovies: RDD[String], sc: SparkContext): (RDD[(String, Int)], RDD[(Int, Int, Double)]) = {
    /**
     * RDD:用户名字，用户index
     */
    val userIDStats = userMovies
      .map(line => line.split(",")(0).trim)
      .distinct()
      .zipWithUniqueId()
      .map(t => (t._1, t._2.toInt))

    /**
     * RDD:用户下标，电影下标，评分
     */
    val formattedRDD = userMovies
      .map(line => {
        val arr = line.split(",")
        (arr(0).trim, (arr(1).trim, arr(2).trim))
      })
      .join(userIDStats)
      .map {
        case (_, ((movieID, rating), userID)) => (userID, movieID.toInt, rating.toDouble)
      }

    (userIDStats, formattedRDD)
  }
}
