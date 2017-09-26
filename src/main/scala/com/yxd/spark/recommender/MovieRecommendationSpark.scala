package com.yxd.spark.recommender

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Try

case class UserMovieRating(userID: Int, movieID: Int, var rating: Double)

/**
  * 实现基于物品最近邻的协同过滤推荐算法
  * 数据下载：
  * https://grouplens.org/datasets/movielens/
  * http://files.grouplens.org/datasets/movielens/ml-20m.zip
  * http://files.grouplens.org/datasets/movielens/ml-latest-small.zip
  * 实现的步骤:
  *   1. 计算物品之间的相似度: 可以使用皮尔逊相关系数、距离的相关公式等等
  *   2. 根据给定的用户id获取推荐列表(怎么得到一个推荐列表矩阵)
  */
object MovieRecommendationSpark {
  def main(args: Array[String]): Unit = {
    // 一、上下文构建
    // 1.1 SparkContext创建
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("MovieRecommendationSpark")
    val sc = SparkContext.getOrCreate(conf)
    // 1.2 数据读取
    val userMoviesRatingRDD: RDD[UserMovieRating] = sc
      .textFile("data/ratings.csv")
      .map(line => {
        val arr = line.split(",")
        Try(UserMovieRating(arr(0).trim.toInt, arr(1).trim.toInt, arr(2).trim.toDouble))
      })
      .filter(_.isSuccess)
      .map(_.get)
    val moviesRDD: RDD[(Int, String)] = sc
      .textFile("data/movies.csv")
      .map(line => {
        val arr = line.split(",")
        Try((arr(0).trim.toInt, arr(1).trim))
      })
      .filter(_.isSuccess)
      .map(_.get)
    userMoviesRatingRDD.cache()

    // 二、物品相似度矩阵计算
    // 2.1 计算出每个用户的评分均值
    val userAvgRatingRDD = userMoviesRatingRDD
      .map(record => (record.userID, record.rating))
      .groupByKey()
      .map {
        case (userID, iter) => {
          // iter中存储的是当前用户id(userID)对应的所有评分数据
          // a. 求评分记录总数以及评分的总和
          val (total, sum) = iter.foldLeft((0, 0.0))((a, b) => {
            val v1 = a._1 + 1
            val v2 = a._2 + b
            (v1, v2)
          })
          // b. 计算平均值并返回
          (userID, 1.0 * sum / total)
        }
      }
    // 2.2 将原始的评分矩阵转换为减去均值的评分矩阵
    val removeAvgRatingUserMoviesRatingRDD = userMoviesRatingRDD
      .map(record => (record.userID, record))
      .join(userAvgRatingRDD)
      .map {
        case (_, (record, avgRating)) => {
          record.rating -= avgRating
          (record.movieID, record)
        }
      }
    // 2.3 计算出每个电影的评分人数
    val numberOfRatersPerMoviesRDD = userMoviesRatingRDD
      .map(record => (record.movieID, 1))
      .reduceByKey(_ + _)
    // 2.4 关联获取每个电影的评分人数
    val userMovieRatingNumberOfRatesRDD = removeAvgRatingUserMoviesRatingRDD
      .join(numberOfRatersPerMoviesRDD)
      .map {
        case (_, (record, raters)) => {
          (record.userID, (record, raters)) //这里格式：(userid,((userid,movieid,rating,removeAvgRate),rater))
        }
      }
    // 2.5 计算出每个用户 所有评分数量，电影的数据
    val groupedByUserIDRDD = userMovieRatingNumberOfRatesRDD.groupByKey()
    // 2.6 计算电影的成对矩阵 余弦数据对比 的情况
    val moviePairsRDD: RDD[((Int, Int), (Double, Int, Double, Int, Double, Double, Double))] = groupedByUserIDRDD
      .flatMap {
        case (userID, iter) => {
          // 从iter中获取电影的成对信息
          // a. 将数据进行一个排序操作（按照电影id进行数据排序）
          val sorted: List[(UserMovieRating, Int)] = iter
            .toList
            .sortBy(_._1.movieID)
          // b. 双层循环获取计算结果并返回
          sorted.flatMap {
            case (UserMovieRating(_, movieID1, rating1), raters1) => {
              sorted
                .filter(_._1.movieID > movieID1)
                .map {
                  case (UserMovieRating(_, movieID2, rating2), raters2) => {
                    // movieID1和movieID2同时出现
                    val key = (movieID1, movieID2)
                    val ratingProduct = rating1 * rating2
                    val movie1RatingSquared = rating1 * rating1
                    val movie2RatingSquared = rating2 * rating2
                    // 返回计算结果
                    (key, (rating1, raters1, rating2, raters2, ratingProduct, movie1RatingSquared, movie2RatingSquared))
                  }
                }
            }
          }
        }
      }


    // 2.7 计算电影的整体的一个评分（也就是物品的相似度矩阵 格式：((movieid1,movieid2),相似度)）
    val movieSimilarityRDD: RDD[(Int, Int, Double)] = moviePairsRDD
      /* 按照(movieID1, movieID2)键值对进行聚合操作*/
      .groupByKey()
      .mapValues(iter => {
        // 计算当前电影组的相似度
        // iter是一个迭代器，内部的数据类型是：(Double, Int, Double, Int, Double, Double, Double)
        // 对于某一个用户来讲， (movie1的用户评分, movie1的总评分人数, movie2的用户评分, movie2的总评分人数, movie1的评分*movie2的评分，movie1的评分^2，movie2的评分^2）
        // a. 合并数据
        val (rating1, numOfRaters1, rating2, numOfRaters2, ratingProduct, rating1Squared, rating2Squared) = iter.foldRight((List[Double](), List[Int](), List[Double](), List[Int](), List[Double](), List[Double](), List[Double]()))((b, a) => {
          (
            b._1 :: a._1,
            b._2 :: a._2,
            b._3 :: a._3,
            b._4 :: a._4,
            b._5 :: a._5,
            b._6 :: a._6,
            b._7 :: a._7
            )
        })
        // b. 开始正式计算相似度
        // b.1 余弦改进公式的计算
        val dotProduct = ratingProduct.sum
        val rating1NormSq = rating1Squared.sum
        val rating2NormSq = rating2Squared.sum
        val adjuestedCosineCorrelation = dotProduct / (math.sqrt(rating1NormSq) * math.sqrt(rating2NormSq))

        // c. 结果输出
        adjuestedCosineCorrelation
      })
      .map {
        case ((movieID1, movieID2), similarity) => {
          (movieID1, movieID2, similarity)
        }
      }
    .sortBy(_._3)//按照相似度排序
    movieSimilarityRDD.cache()


    // 2.8 将物品的评分矩阵输出到某一个位置，比如：文件系统、redis、mysql等等
    // TODO: 自行使用foreachPartition API将数据输出到关系型数据库或者redis中
    movieSimilarityRDD
      .map(t => s"${t._1},${t._2},${t._3}")
      .saveAsTextFile(s"result/movie_similarity/${System.currentTimeMillis()}")

    // 三、用户-物品关联度矩阵计算
    val K = 50//前k个值
    val tol = 3.0 //相似度系数
    // 3.1 获取所有用户id形成的RDD
    val index2UserIDRDD = userMoviesRatingRDD
      .map(record => record.userID)
      .distinct()
      .zipWithIndex()
      .map(_.swap)
    index2UserIDRDD.cache()
    val userNumber = index2UserIDRDD.count()
    // 3.2 获取所有的电影id形成的RDD
    /**
     * 这里格式假如有三个user 一个movie
     * index2MovieIDRDD格式：
     * i1 m1
     * i2 m1
     * i3 m1
     */
    val index2MovieIDRDD: RDD[(Long, Int)] = moviesRDD
      .flatMap {
        case (movieID, movieName) => {
          (0L until userNumber).map(index => (index, movieID))
        }
      }
    // 3.3 数据join
    /**
     * 这里是将所有的电影 和 用户 rdd 关联 得到所有的可能形成的评分
     * record.userID, record.movieID，record.ratings
     */
    val userIDAndMovieIDPairRDD = index2UserIDRDD
      .join(index2MovieIDRDD)
      .map(t => ((t._2._1, t._2._2), None))
    // 3.4 join已经评分过的数据
    /**
     * userMovieRatingRDD2  混合了已经评分了的 和 没有电影评分的矩阵Rdd
     */
    val userMovieRatingRDD2 = userIDAndMovieIDPairRDD
      .leftOuterJoin(userMoviesRatingRDD.map(record => ((record.userID, record.movieID), record.rating)))
      .map {
        case ((userID, movieID), (_, ratingOption)) => {
          // 计算userid对于movieID的评分值
          if (ratingOption.isDefined) {
            // 表示已经评过分数了
            (userID, movieID, ratingOption.get, 0)
          } else {
            // 需要计算评分
            (userID, movieID, 0.0, 1)
          }
        }
      }
    userMovieRatingRDD2.cache()

    // 3.5和物品相似度rdd进行jion得到数据的评分值
    val predictUserMovieRatingRDD = userMovieRatingRDD2
      .filter(_._4 == 1)
      .map(t => (t._2, t._1))//电影id 用户id
      .join(
        movieSimilarityRDD
          .filter(_._3 > tol)//取出相似度大于0.3的rdd
          .flatMap {
            case (movieID1, movieID2, sim) => {
              (movieID1, (movieID2, sim)) :: (movieID2, (movieID1, sim)) :: Nil
            }
          }
      )
      .map {
        case (movieID1, (userID, (movieID2, sim))) => {
          // 求解userID对于movieID1的评分
          ((movieID1, userID), (movieID2, sim))
        }
      }
      .groupByKey()
      .flatMap {
        case ((movieID1, userID), iter) => {
          // 求iter的前K个相似物品来计算最终的一个评分
          val topKMovieSimIter = iter
            .toList
            .sortBy(_._2)
            .takeRight(K)
          topKMovieSimIter.map {
            case (movieID2, sim) => {
              (movieID2, (movieID1, userID, sim))
            }
          }
        }
      }
      .join(userMoviesRatingRDD.map(record => (record.movieID, record.rating)))
      .map {
        case (_, ((movieID, userID, sim), rating)) => {
          ((movieID, userID), (rating, sim))
        }
      }
      .groupByKey()
      .map {
        case ((movieID, userID), iter) => {
          // 计算最终的评分
          val (col1, col2) = iter.foldLeft((0.0, 0.0))((a, b) => {
            val v1 = a._1 + b._1 * b._2
            val v2 = a._2 + b._2
            (v1, v2)
          })
          (userID, movieID, 1.0 * col1 / col2, 1)
        }
      }
    // 3.6 整合最终数据
    val result = predictUserMovieRatingRDD.union(userMovieRatingRDD2.filter(_._4 == 0))
    result.cache()

    // 3.7 将最终的用户、物品评分矩阵进行输出
    result.saveAsTextFile(s"result/user_movie/${System.currentTimeMillis()}")

    // 四、根据给定的用户id获取推荐列表
    val userID = 671
    result
      .filter(t => t._1 == userID && t._4 == 1)
      .mapPartitions(iter => {
        val s: List[(Int, Int, Double, Int)] = iter
          .toList
          .sortBy(_._3)
          .takeRight(10)
        s.toIterator
      })
      .collect()
      .sortBy(_._3)
      .takeRight(10)
      .foreach(println)


    // 开发过程中暂停一下，为了看http://localhost:4040/jobs/
    Thread.sleep(100000)
  }
}
