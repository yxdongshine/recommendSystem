package com.yxd.sparksql.recommend

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, Dataset, DataFrame}
import org.apache.spark.{SparkContext, SparkConf}

import scala.util.Try

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

    val movieTable: DataFrame = sc.textFile("data/movies.csv")
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
    val removeAvgRateDataFrame: DataFrame = umrAvgRateDataFrame.select(umrAvgRateDataFrame("userID"),umrAvgRateDataFrame("movieID"),
      (umrAvgRateDataFrame("rating") - umrAvgRateDataFrame("avgRate")).as("removeAvgRate"),umrAvgRateDataFrame("avgRate"))

    //这里将减去平均值后的矩阵缓存
    removeAvgRateDataFrame.cache()
    //开始根据物品分组 看每组物品有哪些userid
    //先转换成map rdd
    val movieIdRdd: RDD[(Int, Iterable[(Int, Int, Double, Double)])] = removeAvgRateDataFrame.map(
    removeRdd => {(removeRdd(1).toString.toInt,(removeRdd(0).toString.toInt,removeRdd(1).toString.toInt,
      removeRdd(2).toString.toDouble,removeRdd(3).toString.toDouble))}
    ).groupByKey().sortBy(_._1)
   /*   .reduceByKey(
     (iter1 ,iter2)=>{
       iter1.map(
        ele1 =>{(ele1._1 + iter2.head._1,ele1._2 + iter2.head._2,ele1._3 + iter2.head._3,ele1._4 + iter2.head._4)}
       )
    })*/

    movieIdRdd.foreach(println(_))
    //缓存movieidRdd
    movieIdRdd.cache()

    //两次循环物品对应的用户评分 与其他物品的相似度
    val similarityDegreeMatrixSeq: Seq[(Int, Int, Double)] = movieIdRdd.toArray().flatMap{
      case (movieId, removeIter) => {
        val removeList1 = removeIter.toList
        movieIdRdd.toArray()
          .filter( _._1 > movieId )
          .map{
            case (movieId1, removeIter2) => {
              val removeList2 = removeIter2.toList
              var fenzi = 0.0
              var fenmu1 = 0.0
              var fenmu2 = 0.0
              //先以第一个物品计算其他物品分数
              removeList1.map{
                case(userIdrl1,_,rarrl1,_) => {
                  val rarMovie2 = removeList2.find(_._1 == userIdrl1).getOrElse(null)
                  if(null != rarMovie2){
                    fenzi += rarrl1 * rarMovie2._3
                    fenmu1 += rarrl1*rarrl1
                    (fenzi,fenmu1)
                  }
                }
              }

              //再以第N个物品计算第一个物品分数
              removeList2.map{
                case(userIdrl2,_,rarrl2,_) => {
                  val rarMovie1 = removeList1.find(_._1 == userIdrl2).getOrElse(null)
                  if(null != rarMovie1){
                    fenzi += rarrl2 * rarMovie1._3
                    fenmu2 += rarrl2*rarrl2
                    (fenzi,fenmu2)
                  }
                }
              }
              //最后的相似度
              val similarityDegree = fenzi / (math.sqrt(fenmu1) * math.sqrt(fenmu2))
              //返回成物品之间的相似度矩阵
              (movieId,movieId1,similarityDegree)
            }
          }
      }
    }.toSeq

    val similarityDegreeMatrix: RDD[(Int, Int, Double)] = sc.makeRDD(similarityDegreeMatrixSeq)
    //换成相似度similarityDegreeMatrix
    similarityDegreeMatrix.cache()
    //将相似度矩阵保存在方便以后加载重复训练
    similarityDegreeMatrix.foreach(println(_))
    similarityDegreeMatrix
      .map(
         sd => {
           sd._1+","+sd._2+","+sd._3
         }
      )
     // .saveAsTextFile(s"result/movie_similarity/${System.currentTimeMillis()}")


    //物品用户组成二位随机矩阵
    val userIdRow: Array[Row] = removeAvgRateDataFrame.select("userID").distinct().orderBy("userID").collect()

    val userMoiveRelationRdd: RDD[(Int, Int, String)] = movieTable.flatMap {
      case (movieDf) => {
        userIdRow.map{
          case (idRow) => {
            Try((idRow.get(0).toString.toInt, movieDf.getInt(0), movieDf.getString(1)))
          }
        }
          .filter(_.isSuccess)
        .map(_.get)
      }
    }
    //userMoiveRelationRdd.foreach(println(_))

    //前k个
    val k = 5
    //相似度因子
    val sd = 0.1
    //得到关系与物品相似度矩阵
    val relationSDRdd: RDD[(Int, Int, Int, Double)] = userMoiveRelationRdd
    .toDF("userId","movieID","movieName")
    .join(similarityDegreeMatrix.toDF("movieID","movieID1","similarityDegree"),"movieID") //关联相似度
    .map{
      case (row) => {
            //返回格式（userId，movieID，movieID1，similarityDegree）
            (row.get(1).toString.toInt,row.get(0).toString.toInt
              ,row.get(3).toString.toInt,row.get(4).toString.toDouble)
      }
    }
    .filter(rrd => rrd._4.toString.toDouble > sd )

    //得到用户评分矩阵中用户id，电影id与评分值 的键值对
    val umrKV: RDD[((Any, Any), Any)] = sqlContext
      .sql("select userID,movieID,avg(rating) as rating from uwrTable group by userID,movieID ")
    .map{
      case (row ) => {
        ((row.get(0),row.get(1)),row.get(2))
      }
    }

    umrKV.cache()

    //得到前K个
    val recommentItemRdd = relationSDRdd
      .groupBy(rsd =>( rsd._1,rsd._2)).collect()
      .flatMap{
        case (key,cBuffer) => {
          var fenzi = 0.0
          var fenmu = 0.0
          val cbList = cBuffer.toList.sortBy(_._4).takeRight(k)//取出前k个值
          cbList.map{
            case (row) => {
              val umr = umrKV.filter(umrRow => {
                umrRow._1 == (row._1,row._3)
              })
              val sd = row._4.toString.toDouble
              fenzi += umr.map(line =>line._2.toString.toDouble).first() * sd
              fenmu += sd
            }
              val score = fenzi / fenmu * 1.0
              (key._1,key._2,score)
          }

        }
    }


    //展示保存
    recommentItemRdd.foreach(println(_))
    println("=========推荐列表================")
    //推荐用户31的列表
    println("=========推荐用户31列表================")
    recommentItemRdd
    .filter(recommentItem => recommentItem._1 == 31)
    .foreach(println(_))
    // 开发过程中暂停一下，为了看http://localhost:4040/jobs/
    Thread.sleep(100000)
  }


}
