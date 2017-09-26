package com.yxd.sparksql.recommend

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by yxd on 2017/9/26.
 */
case class UserMovieRating(userID: Int, movieID: Int, var rating: Double)
case class Movie( movieID: Int, movieName: String)

object UserMovieRecommend {

  def main(args: Array[String]) {
    // 一、上下文构建
    // 1.1 Sparksql创建
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("UserMovieRecommend")
    val sc = SparkContext.getOrCreate(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    //1.2 数据处理
    val uwrTable = sc.textFile("data/ratings.csv")
      .map( line => {line.split(",")})
      .map( uwrs => {UserMovieRating(uwrs(0).trim.toInt,uwrs(1).toInt,uwrs(2).toDouble)})
      .toDF()
    //注册用户电影评分临时表
    uwrTable.registerTempTable("uwrTable")
    uwrTable.cache()

    val movieTable = sc.textFile("data/movies.csv")
      .map( line => line.split(","))
      .map( ms => { Movie(ms(0).trim().toInt,ms(1).trim)})
      .toDF()
    //注册成电影临时表
    movieTable.registerTempTable("movieTable")
    movieTable.cache()

    //1.3 sql 测试
    val sqlOfUmr = "select * from uwrTable limit 10 "
    sqlContext.sql(sqlOfUmr).foreach(println(_))

    println("###############################")
    val sqlOfM = "select * from movieTable limit 10 "
    sqlContext.sql(sqlOfM).foreach(println(_))



    //二 计算出用户电影评分矩阵
    //2.1算出减去平均分后的矩阵
    //思想: 模型为行用户 列式电影 所以 有根据用户的分组
    val umrAvgRateDataFrame = sqlContext.sql("select userID,avg(rating) as avgRate from uwrTable group by userID ")
      .join(uwrTable,"userID")
    val removeAvgRateDataFrame = umrAvgRateDataFrame.select(umrAvgRateDataFrame("userID"),umrAvgRateDataFrame("movieID"),
      (umrAvgRateDataFrame("rating") - umrAvgRateDataFrame("avgRate")).as("removeAvgRate"),umrAvgRateDataFrame("avgRate"))
      .limit(10)

    //removeAvgRateDataFrame.flatMap()







    // 开发过程中暂停一下，为了看http://localhost:4040/jobs/
    Thread.sleep(100000)
  }


}
