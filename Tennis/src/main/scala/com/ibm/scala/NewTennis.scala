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

object NewTennis extends App {

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

  val conf = new SparkConf().setAppName("NewTennis").setMaster("local")
    case class loc(id: Int, name: String)
  case class prop(id: Int, loc_id:Int, name: String)
  
  val sc = new SparkContext(conf)
  val rootpath = "c:\\workspace\\akka\\"
  val hconf = new Configuration()
  var fs = FileSystem.get(hconf);
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._

  val locations = sc.textFile(rootpath + "locations.csv").map { x => x.split(",") }.map { x => loc(x.apply(0).toInt, x.apply(1)) }.toDF()
  val properties = sc.textFile(rootpath + "properties.csv").map { x => x.split(",") }.map { x => prop(x.apply(0).toInt, x.apply(1).toInt, x.apply(2)) }.toDF()
  
  locations.foreach { println }
/*  val sc = new SparkContext(conf)
  val rootpath = "c:\\solutioninc\\"
  val hconf = new Configuration()
  var fs = FileSystem.get(hconf);
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._

  val dataset = sc.textFile(rootpath + "InitialDatasetRebuiltFull.txt").map { x => x.split(",") }
    .filter { x =>
      (x.apply(18) == "0-0" ||
        x.apply(18) == "1-0" ||
        x.apply(18) == "2-0" ||
        x.apply(18) == "3-0" ||
        x.apply(18) == "4-0" ||
        x.apply(18) == "5-0" ||
        x.apply(18) == "0-1" ||
        x.apply(18) == "0-2" ||
        x.apply(18) == "0-3" ||
        x.apply(18) == "0-4" ||
        x.apply(18) == "0-5" ||
        x.apply(18) == "1-1" ||
        x.apply(18) == "2-1" ||
        x.apply(18) == "3-1" ||
        x.apply(18) == "4-1" ||
        x.apply(18) == "5-1" ||
        x.apply(18) == "1-2" ||
        x.apply(18) == "1-3" ||
        x.apply(18) == "1-4" ||
        x.apply(18) == "1-5" ||
        x.apply(18) == "2-2" ||
        x.apply(18) == "3-2" ||
        x.apply(18) == "4-2" ||
        x.apply(18) == "5-2" ||
        x.apply(18) == "2-3" ||
        x.apply(18) == "2-4" ||
        x.apply(18) == "2-5" ||
        x.apply(18) == "3-3" ||
        x.apply(18) == "4-3" ||
        x.apply(18) == "5-3" ||
        x.apply(18) == "3-4" ||
        x.apply(18) == "3-5" ||
        x.apply(18) == "4-4" ||
        x.apply(18) == "5-4" ||
        x.apply(18) == "4-5" ||
        x.apply(18) == "5-5" ||
        x.apply(18) == "6-5" ||
        x.apply(18) == "5-6" ||
        x.apply(18) == "6-6") &&
        x.apply(24) != "2"
    }

  val WinnerPointDatasetLP =  dataset.filter { x => x.apply(18) == "0-0" && x.apply(14) == "0-0"}.map { x =>
    var ao = 0.0
    var fo = 0.0
    var uo = 0.0
    var w = 0.0
    var clay = 0.0
    var grass = 0.0
    var hard = 0.0
    var man = 0.0
    var woman = 0.0
    var s1 = 0.0
    var s2 = 0.0
    var df = 0.0
    var s0_0 = 0.0
    var s15_0 = 0.0
    var s30_0 = 0.0
    var s40_0 = 0.0
    var s0_15 = 0.0
    var s0_30 = 0.0
    var s0_40 = 0.0
    var s15_15 = 0.0
    var s15_30 = 0.0
    var s15_40 = 0.0
    var s30_15 = 0.0
    var s40_15 = 0.0
    var s30_30 = 0.0
    var s40_30 = 0.0
    var s30_40 = 0.0
    var s40_40 = 0.0
    var s45_40 = 0.0
    var s40_45 = 0.0

    var ss0_0 = 0.0
    var ss1_0 = 0.0
    var ss2_0 = 0.0
    var ss3_0 = 0.0
    var ss4_0 = 0.0
    var ss5_0 = 0.0
    var ss0_1 = 0.0
    var ss0_2 = 0.0
    var ss0_3 = 0.0
    var ss0_4 = 0.0
    var ss0_5 = 0.0
    var ss1_1 = 0.0
    var ss2_1 = 0.0
    var ss3_1 = 0.0
    var ss4_1 = 0.0
    var ss5_1 = 0.0
    var ss1_2 = 0.0
    var ss1_3 = 0.0
    var ss1_4 = 0.0
    var ss1_5 = 0.0
    var ss2_2 = 0.0
    var ss3_2 = 0.0
    var ss4_2 = 0.0
    var ss5_2 = 0.0
    var ss2_3 = 0.0
    var ss2_4 = 0.0
    var ss2_5 = 0.0
    var ss3_3 = 0.0
    var ss4_3 = 0.0
    var ss5_3 = 0.0
    var ss3_4 = 0.0
    var ss3_5 = 0.0
    var ss4_4 = 0.0
    var ss5_4 = 0.0
    var ss4_5 = 0.0
    var ss5_5 = 0.0
    var ss6_5 = 0.0
    var ss5_6 = 0.0
    var ss6_6 = 0.0

    x.apply(1).toDouble match {
      case 1 => ao = 1.0
      case 2 => fo = 1.0
      case 3 => uo = 1.0
      case 4 => w = 1.0
    }
    x.apply(2).toDouble match {
      case 1 => hard = 1.0
      case 2 => grass = 1.0
      case 3 => clay = 1.0
    }
    x.apply(3).toDouble match {
      case 0 => woman = 1.0
      case 1 => man = 1.0
    }
    x.apply(13).toDouble match {
      case 1 => s1 = 1.0
      case 2 => s2 = 1.0
      case 0 => df = 1.0
    }

    x.apply(14) match {
      case "0-0" => s0_0 = 1.0
      case "15-0" => s15_0 = 1.0
      case "30-0" => s30_0 = 1.0
      case "40-0" => s40_0 = 1.0
      case "0-15" => s0_15 = 1.0
      case "0-30" => s0_30 = 1.0
      case "0-40" => s0_40 = 1.0
      case "15-15" => s15_15 = 1.0
      case "30-15" => s30_15 = 1.0
      case "40-15" => s40_15 = 1.0
      case "15-30" => s15_30 = 1.0
      case "15-40" => s15_40 = 1.0
      case "30-30" => s30_30 = 1.0
      case "40-30" => s40_30 = 1.0
      case "30-40" => s30_40 = 1.0
      case "40-40" => s40_40 = 1.0
      case "45-40" => s45_40 = 1.0
      case "40-45" => s40_45 = 1.0
    }

    x.apply(18) match {
      case "0-0" => ss0_0 = 1.0
      case "1-0" => ss1_0 = 1.0
      case "2-0" => ss2_0 = 1.0
      case "3-0" => ss3_0 = 1.0
      case "4-0" => ss4_0 = 1.0
      case "5-0" => ss5_0 = 1.0
      case "0-1" => ss0_1 = 1.0
      case "0-2" => ss0_2 = 1.0
      case "0-3" => ss0_3 = 1.0
      case "0-4" => ss0_4 = 1.0
      case "0-5" => ss0_5 = 1.0
      case "1-1" => ss1_1 = 1.0
      case "2-1" => ss2_1 = 1.0
      case "3-1" => ss3_1 = 1.0
      case "4-1" => ss4_1 = 1.0
      case "5-1" => ss5_1 = 1.0
      case "1-2" => ss1_2 = 1.0
      case "1-3" => ss1_3 = 1.0
      case "1-4" => ss1_4 = 1.0
      case "1-5" => ss1_5 = 1.0
      case "2-2" => ss2_2 = 1.0
      case "3-2" => ss3_2 = 1.0
      case "4-2" => ss4_2 = 1.0
      case "5-2" => ss5_2 = 1.0
      case "2-3" => ss2_3 = 1.0
      case "2-4" => ss2_4 = 1.0
      case "2-5" => ss2_5 = 1.0
      case "3-3" => ss3_3 = 1.0
      case "4-3" => ss4_3 = 1.0
      case "5-3" => ss5_3 = 1.0
      case "3-4" => ss3_4 = 1.0
      case "3-5" => ss3_5 = 1.0
      case "4-4" => ss4_4 = 1.0
      case "5-4" => ss5_4 = 1.0
      case "4-5" => ss4_5 = 1.0
      case "5-5" => ss5_5 = 1.0
      case "6-5" => ss6_5 = 1.0
      case "5-6" => ss5_6 = 1.0
      case "6-6" => ss6_6 = 1.0
      case default =>
    }
    LabeledPoint(
      //         x.apply(17).toDouble, /*win point*/
      //   x.apply(20).toDouble, /*win game*/
//        x.apply(24).toDouble, /*win set*/
      x.apply(28).toDouble, win match
//      x.apply(26).toDouble, /*total games in match*/
      Vectors.dense(
//        ao, fo, uo, w, 
        hard, grass, clay, 
        woman, man 
        set/game , x.apply(6).toDouble, x.apply(7).toDouble, 
 server returner       x.apply(9).hashCode().toDouble, x.apply(11).hashCode().toDouble 
        ranks , x.apply(10).toDouble, x.apply(12).toDouble 
        ,s1, s2, df
        ,s0_0, s15_0, s30_0, s40_0, s0_15, s0_30, s0_40, s15_15, s15_30, s15_40, s30_15, s40_15, s30_30, s40_30, s30_40, s40_40, s45_40, s40_45
        ,ss0_0, ss1_0, ss2_0, ss3_0, ss4_0, ss5_0, ss0_1, ss0_2, ss0_3, ss0_4, ss0_5, ss1_1, ss2_1, ss3_1, ss4_1, ss5_1, ss1_2, ss1_3, ss1_4, ss1_5, ss2_2, ss3_2, ss4_2, ss5_2, ss2_3, ss2_4, ss2_5, ss3_3, ss4_3, ss5_3, ss3_4, ss3_5, ss4_4, ss5_4, ss4_5, ss5_5, ss6_5, ss5_6, ss6_6 
//        /*1point*/ , x.apply(30).toDouble 
        did break , x.apply(31).toDouble 
         win game after 0-40  , x.apply(32).toDouble
        )
        )
  }.toDF("label", "features")

  val lr = new LogisticRegression()
  lr.setMaxIter(10)
    .setRegParam(0.01)
   
  val model1 = lr.fit(WinnerPointDatasetLP)

  val paramMap = ParamMap(lr.maxIter -> 20)
    .put(lr.maxIter, 30) // Specify 1 Param.  This overwrites the original maxIter.
    .put(lr.regParam -> 0.1, lr.threshold -> 0.55) // Specify multiple Params.

  val m1 = model1.transform(WinnerPointDatasetLP)
    .select("label", "probability", "prediction").map { x =>
      val probabilities = x.apply(1).toString.substring(1).dropRight(1).split(",").map(x => x.toDouble)
      (x.apply(0).toString, probabilities.max, x.apply(2).toString)
    }
  val percentage1 = m1.filter(f => f._1 == f._3).count().toDouble / WinnerPointDatasetLP.count().toDouble * 100

  val aveProb = m1.filter(f => f._1 == f._3)
  var s1 = 0.0

  var count = aveProb.count()
  aveProb.foreach { f =>
    s1 += f._2
  }
  val percentage2 = m1.filter(f => f._1 == f._3 && f._2 > 0.7).count().toDouble / m1.filter(f => f._2 > 0.7).count().toDouble * 100

  println("prediction is right in " + percentage1 + " percentages")
  println("prediction is right in " + percentage2 + " percentages")
*/
}