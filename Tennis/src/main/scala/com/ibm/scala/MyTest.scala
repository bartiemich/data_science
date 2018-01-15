package main.scala.com.ibm.scala

import org.apache.spark.mllib.classification.{ NaiveBayes, NaiveBayesModel }
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import java.io._
import java.util.Calendar
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import java.text.SimpleDateFormat
import java.util.Date;
import java.util.TimeZone

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.mllib.stat.{ MultivariateStatisticalSummary, Statistics }
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.optimization.{ SimpleUpdater, SquaredL2Updater, L1Updater }
import org.apache.spark.sql.{ Row, SQLContext }
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.regression.LinearRegressionWithSGD

object MyTest extends App {

  case class Data(
    tournament: Int, // 1 - AO, 2- RG, 3 - US, 4 - W
    surface: Int, //1 - hard, 2 - grass, 3 - clay
    gender: Int, // 0 - W, 1 - M  
    matchNum: Int,
    year: Int,
    players: String, // pair of players
    matchWinner: String,
    set: Int,
    game: Int,
    server: String,
    returner: String,
    serveNumber: Int, // 1 - 1, 2 - 2, 0 - DOubleFault 
    winnerScoreBeforeGame: Int,
    opponentScoreBeforeGame: Int,
    winnerScoreAfterGame: Int,
    opponentScoreAfterGame: Int,
    pointWinner: String,
    isServerWin: Int //1 - server 2 returner
    ) {
    override def toString(): String =
      tournament + "," + surface + "," + year + "," + gender + "," + +matchNum + "," + players + "," + matchWinner + "," + set + "," + game + "," + server + "," + returner + "," + serveNumber + "," + winnerScoreBeforeGame /*.toString().replace("45", "A")*/ + "-" + opponentScoreBeforeGame /*.toString().replace("45", "A")*/ + "," + winnerScoreAfterGame /*.toString().replace("45", "A")*/ + "-" + opponentScoreAfterGame /*.toString().replace("45", "A")*/ + "," + pointWinner + "," + isServerWin
  }

  def getTournament(tournament: String) = {
    if (tournament == "Australian Open") 1
    else if (tournament == "French Open" || tournament == "Roland Garros") 2
    else if (tournament == "US Open") 3
    else if (tournament == "Wimbledon") 4
    else 0
  }

  def getSurface(tournament: String) = {
    if ((tournament == "Australian Open") || (tournament == "US Open") || (tournament == "1") || (tournament == "3")) 1
    else if (tournament == "Wimbledon" || (tournament == "4")) 2
    else if (tournament == "French Open" || tournament == "Roland Garros" || (tournament == "2")) 3
    else 0
  }

  def getSex(sex: String) = {
    if (sex == "Male") 1
    else 0
  }
  def getGame(game: Int, score: String) = {
    if (score == "0-0") game - 1
    else game
  }

  def getPointType(pointType: String) = {
    if (pointType == "Game point") 1
    else if (pointType == "Break point") 2
    else if (pointType == "Set point") 3
    else if (pointType == "Match Point") 4
    else 0
  }

  def getServeNumber(serveType: String) = {
    if (serveType == "First serve") 1
    else if (serveType == "Second serve") 2
    else 0
  }

  def getPlayers(first: String, second: String) = {
    val list = List(first.toUpperCase(), second.toUpperCase()).sorted
    list.mkString("-")
  }

  def getScore(score: String) = {
    if (score != "0") {
      val res = score.replaceAll("A", "45").split("-").map { x => x.toInt }
      (res.apply(0), res.apply(1))
    } else (0, 0)
  }

  def getpointWinner(matchWinner: String, serverName: String, returnerName: String, winnerScoreBeforeGame: Int, opponentScoreBeforeGame: Int, winnerScoreAfterGame: Int, opponentScoreAfterGame: Int) = {
    //   var res = List((matchWinner, 2))
    var winner = matchWinner
    var winnerType = 0

    var isWinnerServer = false

    if (matchWinner == serverName) {
      isWinnerServer = true
    }

    var isWinnerWinGame = false
    if (winnerScoreAfterGame == 0 && opponentScoreAfterGame == 0) {
      if (winnerScoreBeforeGame > opponentScoreBeforeGame)
        isWinnerWinGame = true
    } else {
      if ((winnerScoreBeforeGame - opponentScoreBeforeGame) < (winnerScoreAfterGame - opponentScoreAfterGame))
        isWinnerWinGame = true
    }

    if (!isWinnerWinGame) {
      if (isWinnerServer) {
        winner = returnerName
      } else {
        winner = serverName
        winnerType = 1
      }
    } else {
      if (isWinnerServer) {
        winnerType = 1
      }
    }

    (winner, winnerType)
  }

  def saveOneFile(filename: String) = {
    FileUtil.copyMerge(
      fs, new Path(filename),
      fs, new Path(filename + ".txt"),
      true,
      fs.getConf, null)
  }

  def getPlayerRate(tournament: Int, year: Int, name: String, rate: Int) = {
    var playerRate = rate
    if (name == "Santiago GONZALEZ") playerRate = 100
    if (name == "Rhyne WILLIAMS") {
      if (year == 2012) playerRate = 47
      if (year == 2013) playerRate = 24
      if (year == 2014) playerRate = 37
    }
    playerRate
  }

  def getScores(scores: List[String]) = {
    var res1 = 0
    var res2 = 0
    scores.foreach { x =>
      val s = x.split("-")
      if (s.apply(0).toInt > s.apply(1).toInt)
        res1 += 1
      else {
        res2 += 1
      }
    }

    (res1, res2)
  }

  def getPlayerWin(before1: Int, before2: Int, after1: Int, after2: Int, server: String, returner: String) = {
    var res = "N/A"
    if (before1 == after1 && before2 < after2) {
      res = returner
    }

    if (before1 < after1 && before2 == after2) {
      res = server
    }
    res
  }

  val conf = new SparkConf().setAppName("MyTest").setMaster("local")
  val sc = new SparkContext(conf)
  val rootpath = "c:\\solutioninc\\"
  val hconf = new Configuration()
  var fs = FileSystem.get(hconf);
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._

    val dataset = sc.textFile(rootpath + "InitialDatasetRebuiltFull.txt").map { x => x.split(",") }

  val d1 = dataset.filter { x => x.apply(14) == "45-40" }
    .map { x => ((x.apply(0), x.apply(6), x.apply(7)), 1) }
    .distinct()

  val d2 = d1.leftOuterJoin(dataset.map { x => ((x.apply(0), x.apply(6), x.apply(7)), x) })
    .filter(f => f._2._2.isDefined)
    .map(f => f._2._2.get)
    .filter { x =>
      val score = x.apply(14).split("-").map { x => x.toInt }
      score.apply(0) < score.apply(1) && score.apply(1) == 40
    }
    .map { x => ((x.apply(0), x.apply(6)), x.apply(7).toInt) }
    .distinct()

  val d3 = dataset.map { x => ((x.apply(0), x.apply(6)), x.apply(7).toInt) }
    .groupByKey
    .map { x => (x._1, x._2.max) }

  val d4 = d2.leftOuterJoin(d3)
    .filter(f => f._2._2.isDefined)
    .map { x =>
      var set = x._1._2.toInt
      var game = x._2._1

      if (game == x._2._2.get) {
        set += 1
        game = 1
      } else {
        game += 1
      }
      ((x._1._1, set, game), 1)
    }
   d4.saveAsTextFile(rootpath + "InitialDatasetRebuiltFull2--")
    saveOneFile(rootpath + "InitialDatasetRebuiltFull2--")
}