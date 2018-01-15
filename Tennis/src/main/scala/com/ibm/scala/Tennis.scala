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
import org.apache.spark.sql.SQLContext;

object Tennis extends App {

  case class Data(
    tournament: Int, // 1 - AO, 2- RG, 3 - US, 4 - W
    surface: Int, //1 - hard, 2 - grass, 3 - clay
    gender: Int, // 0 - W, 1 - M    
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
    isServerWin: Int, //1 - server 2 returner
    pointType: Int, // 0 - unknown, 3 - setpoint, 2 - breakpoint, 4 - matchpoint, 1 - gamepoint
    sRate: Int,
    rRate: Int) {
    override def toString(): String =
      tournament + "," + surface + "," + year + "," + gender + "," + players + "," + matchWinner + "," + set + "," + game + "," + server + "," + returner + "," + serveNumber + "," + winnerScoreBeforeGame /*.toString().replace("45", "A")*/ + "-" + opponentScoreBeforeGame /*.toString().replace("45", "A")*/ + "," + winnerScoreAfterGame /*.toString().replace("45", "A")*/ + "-" + opponentScoreAfterGame /*.toString().replace("45", "A")*/ + "," + pointWinner + "," + isServerWin + "," + pointType + "," + sRate + "," + rRate
  }

  def getTournament(tournament: String) = {
    if (tournament == "Australian Open") 1
    else if (tournament == "French Open") 2
    else if (tournament == "US Open") 3
    else if (tournament == "Wimbledon") 4
    else 0
  }

  def getSurface(tournament: String) = {
    if ((tournament == "Australian Open") || (tournament == "US Open")) 1
    else if (tournament == "Wimbledon") 2
    else if (tournament == "French Open") 3
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
    val list = List(first, second).sorted
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

  def getRanks(matchWinner: String, server: String, returner: String, r1: Int, r2: Int, wr: String, lr: String) = {
    var isWinnerServer = false
    if (matchWinner == server) {
      isWinnerServer = true
    }
    var rank1 = r1
    var rank2 = r2
    if (!lr.contains("A") && !wr.contains("A") && wr.length() > 2 && wr.length() > 2) {
      if (lr.replace("(", "").toInt != 0 && lr.replace("(", "").toInt != 0) {
        if (r1 == 111)
          if (isWinnerServer) {
            if (wr.replace("(", "").toInt < 400)
              rank1 = wr.replace("(", "").toInt
          } else {
            if (lr.replace("(", "").toInt < 400)
              rank1 = lr.replace("(", "").toInt
          }

        if (r2 == 111)
          if (isWinnerServer) {
            if (lr.replace("(", "").toInt < 400)
              rank2 = lr.replace("(", "").toInt
          } else {
            if (wr.replace("(", "").toInt < 400)
              rank2 = wr.replace("(", "").toInt
          }
      }
    }
    (rank1, rank2)
  }

  def getSetInfo(matchWinner: String, server: String, returner: String, score: String, set: Int) = {
    var s1 = 0
    var s2 = 0
    var setWinnerName = matchWinner

    if (score != "0") {
      val ss = score.replace("RET", "").trim().split(" ")
      if (ss.size >= set) {
        val s = ss.apply(set - 1).split("-")
        if (!s.apply(0).isEmpty())
          s1 = s.apply(0).toInt
        if (s.size > 1) {
          if (!s.apply(1).isEmpty())
            s2 = s.apply(1).toInt
        }
        if (s1 < s2) {
          if (matchWinner != server)
            setWinnerName = returner
        }
      }
    }
    (setWinnerName, s1 + "-" + s2, s1 + s2)
  }

  def getSetScore(score: String, set: Int) = {
    var s1 = 0
    var s2 = 0
    if (score != "0") {
      val ss = score.replace("RET", "").trim().split(" ")
      if (ss.size >= set) {
        val s = ss.apply(set - 1).split("-")
        if (!s.apply(0).isEmpty())
          s1 = s.apply(0).toInt
        if (s.size > 1) {
          if (!s.apply(1).isEmpty())
            s2 = s.apply(1).toInt
        }
      }
    }
     (s1 + "-" + s2)
  }

  def getTotalGames(score: String) = {
    var sum = 0
    if (score != "0") {
      val s = score.replace("RET", "").replace("-", " ").trim().split(" ")
      s.foreach { x =>
        if(!x.isEmpty())
        sum += x.toInt
      }
    }
    sum
  }

  def modifyScore(score: String) = {
    var res = ""
    if (score.contains(";")) {
      val s = score.replace("(", "").split(";").filter { x => x.contains("-") }.mkString(" ")
      res = s
    } else {
      res = score.replace("(0)", "").replace("(1)", "").replace("(2)", "").replace("(3)", "").replace("(4)", "").replace("(5)", "").replace("(6)", "").replace("(7)", "").replace("(8)", "").replace("(9)", "").replace("(10)", "").replace("(11)", "").replace("(12)", "").replace("(13)", "")
    }

    res
  }

 def getGameWinner(server: String, returner: String, b1: Int, b2: Int, a1: Int, a2: Int, setWinner: String, setScoreReal: String, isSetWinnerServer: Int, setScoreSideBySide: String, setScoreServerReturner: String ) = {
   var typeOfCalc = 1 /* side by side*/
   var winnerSide = 1
   var gameWinnerName = setWinner
   var win = 1
   var isGameWinnerServer = 1
   val setLoser = if (setWinner == server) returner else server
   if(setScoreServerReturner == setScoreReal)
   {
     typeOfCalc = 0 /* Server - Returner*/
   }
   else{
      if(setScoreSideBySide.split("-").apply(0).toInt < setScoreSideBySide.split("-").apply(1).toInt)
       winnerSide = 2
   }
   
   if(b1 == a1 && b2 < a2) /*2 win*/
     win = 2
   if(b1 < a1 && b2 == a2) /*1 win*/
     win = 1

  if(b1 > b2 && a1 == 0 && a2 == 0) /*1 win*/
     win = 1
  if(b1 < b2 && a1 == 0 && a2 == 0) /*2 win*/
     win = 2

   if(typeOfCalc == 0){
     if(win == 2){
       gameWinnerName = returner
     }
     else
       gameWinnerName = server
   }
   else{
     if(winnerSide == 2){
       if(win == 2){
         gameWinnerName = setWinner
       }
       else
         gameWinnerName = setLoser
     }
     else{
        if(win == 2){
         gameWinnerName = setLoser
       }
       else
         gameWinnerName = setWinner
  
     }
   }
   
   if(gameWinnerName == setWinner) {
     if(isSetWinnerServer == 0)
     isGameWinnerServer = 0
   }
   else {
    if(isSetWinnerServer == 1)
     isGameWinnerServer = 0
   }
   
   (gameWinnerName, isGameWinnerServer)
 }

  val conf = new SparkConf().setAppName("Tennis").setMaster("local")
  val sc = new SparkContext(conf)
  val rootpath = "c:\\solutioninc\\"
  val hconf = new Configuration()
  var fs = FileSystem.get(hconf);
  
  val dataset = sc.textFile(rootpath + "Slams_Points_New.csv").map { x => x.split(",") }.filter { x => x.apply(0) != "Tournament" }
  val dataset2 = sc.textFile(rootpath + "MainResultDataset.txt").map { x => x.substring(1).dropRight(1).split(",") }
  
  val dataset3 = dataset.filter { x => x.apply(7).isEmpty() || x.apply(8).isEmpty() || x.apply(16).isEmpty() }
  val dataset4 = dataset2.filter { x => x.apply(16) == "0" }
  val dataset5 = dataset2.filter { x => x.apply(21) == x.apply(24) || x.apply(21) == x.apply(25) }
  val dataset6 = dataset5.filter { x => x.apply(21) != x.apply(24) && x.apply(21) == x.apply(25) }

  
  val overallCount = dataset.count()
  val newCount = dataset2.count()
  val zeroResultCount = dataset4.count()
  val zeroPlayerCount = dataset3.count()
  val exactSetREsultCount = dataset5.count()
  val onlyServerReturnerCount = dataset6.count()
  println(overallCount)
  println(newCount)
  println(zeroResultCount)
  println(zeroPlayerCount)
  println(exactSetREsultCount)
  println(onlyServerReturnerCount)
  
 
  
 /*  
  val dataset = sc.textFile(rootpath + "joinedResult3.txt").map { x => x.substring(1).dropRight(1).split(",") }
  .map{x =>
          (x.apply(0).toInt, x.apply(1).toInt,x.apply(2).toInt,x.apply(3).toInt,x.apply(4).toUpperCase() ,x.apply(5).toInt, x.apply(6).toInt, x.apply(7).toUpperCase(),x.apply(8).toUpperCase() ,x.apply(9).toInt,
          x.apply(10).toInt, x.apply(11).toInt, x.apply(12).toInt,x.apply(13).toInt,
          x.apply(14).toInt+ "," + x.apply(15).toInt+ "," + x.apply(16)+ "," + x.apply(17).toUpperCase()+ "," +
          x.apply(18) + "," +
          x.apply(19) + "," +
          x.apply(20) + "," +
          x.apply(21) + "," +
          x.apply(22) + "," +
          x.apply(23) + "," +
          x.apply(24) + "," +
          x.apply(25)  + "," +
          x.apply(26)  + "," +
          x.apply(27)  
    
  )
  }
   .sortBy(x => (x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._11, x._12), true)
 
   dataset.saveAsTextFile(rootpath + "MainResultDataset")
  saveOneFile(rootpath + "MainResultDataset")
  
 
    val dataset = sc.textFile(rootpath + "joinedResult2.txt").map { x => x.split(",") }
  .map { x => 
    val winner = getGameWinner(x.apply(7), x.apply(8), x.apply(10).toInt, x.apply(11).toInt, x.apply(12).toInt, x.apply(13).toInt,  x.apply(20), x.apply(21), x.apply(22).toInt,  x.apply(24), x.apply(25))
    (x.mkString(","), winner._1, winner._2)
    }
  dataset.saveAsTextFile(rootpath + "joinedResult3")
  saveOneFile(rootpath + "joinedResult3")
*/
  
 /* 
  val dataset = sc.textFile(rootpath + "joinedResult.txt").map { x => x.substring(1).dropRight(1).split(",") }
  .map { x =>         
          x.apply(0).toInt+ "," + x.apply(1).toInt+ "," + x.apply(2).toInt+ "," + x.apply(3).toInt+ "," + x.apply(4).toUpperCase() + "," + x.apply(6).toInt+ ","  + x.apply(8).toInt+ "," + x.apply(9).toUpperCase() + "," + x.apply(10).toUpperCase() + "," + x.apply(11).toInt+ "," +
          x.apply(12).toInt+ "," + x.apply(13).toInt+ "," + x.apply(14).toInt+ "," + x.apply(15).toInt+ "," +
          x.apply(16).toInt+ "," + x.apply(17).toInt+ "," + x.apply(18)+ "," + x.apply(19).toUpperCase()+ "," +
          x.apply(20) + "," +
          x.apply(21) + "," +
          x.apply(22) + "," +
          x.apply(23) + "," +
          x.apply(24) + "," +
          x.apply(25) + "," +
          getSetScore(x.apply(26), x.apply(6).toInt) + "," +
          getSetScore(x.apply(27), x.apply(6).toInt)
  }
  
  dataset.saveAsTextFile(rootpath + "joinedResult2")
  saveOneFile(rootpath + "joinedResult2")
 */ 
  
 /* 
  val dataset = sc.textFile(rootpath + "resultDataset7.txt").map { x => x.split(",") }
  .map { x => ((x.apply(0).toInt+ "," + x.apply(1).toInt+ "," + x.apply(2).toInt+ "," + x.apply(3).toInt+ "," + x.apply(4).toUpperCase()), x.mkString(",")) }
  
  val datasetSideBySide = sc.textFile(rootpath + "calculatedResultsSideBySide.txt").map { x => x.substring(1).dropRight(1).split(",") }
  .map { x => ((x.apply(0).toInt+ "," + x.apply(1).toInt+ "," + x.apply(2).toInt+ "," + x.apply(3).toInt+ "," + x.apply(4).toUpperCase()), x.apply(5)) }
  
  val datasetServerReturner = sc.textFile(rootpath + "calculatedResultsServerReturner.txt").map { x => x.substring(1).dropRight(1).split(",") }
  .map { x => ((x.apply(0).toInt+ "," + x.apply(1).toInt+ "," + x.apply(2).toInt+ "," + x.apply(3).toInt+ "," + x.apply(4).toUpperCase()), x.apply(5)) }
  
  
  val join = dataset.leftOuterJoin(datasetSideBySide).leftOuterJoin(datasetServerReturner)
  .map(f => 
    (f._2._1._1, getSetScore(f._2._1._2.getOrElse("0"), 1), getSetScore(f._2._2.getOrElse("0"), 1)))
  .sortBy(x => (x._1), true)
   join.saveAsTextFile(rootpath + "joinedResult")
  saveOneFile(rootpath + "joinedResult")
  */
 /* 
    val dataset = sc.textFile(rootpath + "resultDataset7.txt").map { x => x.split(",") }
  .filter { x => x.apply(7).toInt != x.apply(8).toInt }  
  .map {
      x =>
        var winnerGame = x.apply(9)
        if(x.apply(12).toInt < x.apply(13).toInt)
          winnerGame = x.apply(10)
           var s1 = 0
           var s2 = 0
           if(winnerGame == x.apply(19))
             s1 = 1
             else s2= 1
         (
          (x.apply(0).toInt+ "," + x.apply(1).toInt+ "," + x.apply(2).toInt+ "," + x.apply(3).toInt+ "," + x.apply(4).toUpperCase(), x.apply(6).toInt), (s1,s2)
         )
    }
    .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
    .map{x => (x._1._1, (x._1._2, x._2._1 + "-" + x._2._2))}
    .groupByKey
    .map{x => (x._1, x._2.toList.sortBy(f => f._1).map(x => x._2).mkString(" "))}
  dataset.saveAsTextFile(rootpath + "calculatedResultsServerReturner")
  saveOneFile(rootpath + "calculatedResultsServerReturner")
*/

  
 /* 
   val dataset = sc.textFile(rootpath + "resultDataset7.txt").map { x => x.split(",") }
  .filter { x => x.apply(7).toInt != x.apply(8).toInt }  
  .map {
      x =>
        var s1 = 0
        var s2 = 0
        if(x.apply(12).toInt > x.apply(13).toInt)
          s1 = 1
        else s2 = 1
         (
          (x.apply(0).toInt+ "," + x.apply(1).toInt+ "," + x.apply(2).toInt+ "," + x.apply(3).toInt+ "," + x.apply(4).toUpperCase(), x.apply(6).toInt), (s1,s2))
    }
    .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
    .map{x => (x._1._1, (x._1._2, x._2._1 + "-" + x._2._2))}
    .groupByKey
    .map{x => (x._1, x._2.toList.sortBy(f => f._1).map(x => x._2).mkString(" "))}
  dataset.saveAsTextFile(rootpath + "calculatedResultsSideBySide")
  saveOneFile(rootpath + "calculatedResultsSideBySide")
 */ 
 /*
  val dataset = sc.textFile(rootpath + "resultDataset6.txt").map { x => x.split(",") }
  .map {
      x =>
        var setWinnerName = x.apply(19).toUpperCase()
        val setScore = x.apply(23).split("-")
        if(setScore.apply(0).toInt < setScore.apply(1).toInt){
          if(x.apply(20) == "0")
            setWinnerName = x.apply(9).toUpperCase()
            else
            setWinnerName = x.apply(10).toUpperCase()
        }
        var isSetWinnerServer = 0
        if (setWinnerName == x.apply(9).toUpperCase())
          isSetWinnerServer = 1
        (
          x.apply(0).toInt+ "," + x.apply(1).toInt+ "," + x.apply(2).toInt+ "," + x.apply(3).toInt+ "," + x.apply(4).toUpperCase() + "," + x.apply(5).toInt+ "," + x.apply(6).toInt+ "," + x.apply(7).toInt+ "," + x.apply(8).toInt+ "," + x.apply(9).toUpperCase() + "," + x.apply(10).toUpperCase() + "," + x.apply(11).toInt+ "," +
          x.apply(12).toInt+ "," + x.apply(13).toInt+ "," + x.apply(14).toInt+ "," + x.apply(15).toInt+ "," +
          x.apply(16).toInt+ "," + x.apply(17).toInt+ "," + x.apply(18)+ "," + x.apply(19).toUpperCase()+ "," +
          x.apply(20) + "," +
          x.apply(21) + "," +
          setWinnerName/*x.apply(22)*/ + "," +
          x.apply(23) + "," +
          isSetWinnerServer/*x.apply(24)*/ + "," +
          x.apply(25))

    }
  
 dataset.saveAsTextFile(rootpath + "resultDataset7")
  saveOneFile(rootpath + "resultDataset7")
 */ 
  
 /*      val dataset = sc.textFile(rootpath + "resultDataset5.txt").map { x => x.substring(1).dropRight(1).split(",") }
    .map {
      x =>
        (
          x.apply(0).toInt, x.apply(1).toInt, x.apply(2).toInt, x.apply(3).toInt, x.apply(4), x.apply(5).toInt, x.apply(6).toInt, x.apply(7).toInt, x.apply(8), x.apply(9), x.apply(10).toInt,
          x.apply(11).toInt, x.apply(12).toInt, x.apply(13).toInt, x.apply(14).toInt,
          x.apply(15).toInt, x.apply(16).toInt, x.apply(17), x.apply(18),
          x.apply(19) + "," +
          x.apply(20) + "," +
          x.apply(21) + "," +
          x.apply(22) + "," +
          x.apply(23) + "," +
          x.apply(24))

    }
    .sortBy(x => (x._1, x._2, x._3, x._4, x._5, x._7, x._8, x._12, x._13), true)

    var list = dataset.map { x => x._7 }.collect().toList

   list = list.tail
   var list2 = dataset.map { x => x._8 }.collect().toList

   list2 = list2.tail

  val dataset2 = dataset.map { x =>
     val nextSet = list.head
     if(list.size > 1)
     list = list.tail
     val nextGame = list2.head
     if(list2.size > 1)
     list2 = list2.tail
    (x._1 + "," +  x._2 + "," +  x._3 + "," +  x._4 + "," +  x._5 + "," +  nextSet + "," +  x._7 + "," + nextGame + "," +   x._8 + "," +  x._9 + "," +  x._10 + "," +  x._11 + "," +  x._12 + "," +  x._13 + "," +  x._14 + "," +  x._15 + "," +  x._16 + "," +  x._17 + "," +  x._18 + "," +  x._19 + "," +  x._20)
  }
    
  dataset2.saveAsTextFile(rootpath + "resultDataset6")
  saveOneFile(rootpath + "resultDataset6")

  

    val dataset = sc.textFile(rootpath + "resultDataset4.txt").map { x => x.substring(1).dropRight(1).split(",") }
    .map {
      x =>
        var isMatchWinnerServer = 0
        if (x.apply(5).toUpperCase() == x.apply(9).toUpperCase())
          isMatchWinnerServer = 1
        (
          x.apply(0).toInt, x.apply(1).toInt, x.apply(2).toInt, x.apply(3).toInt, x.apply(4), x.apply(6).toInt, x.apply(7).toInt, x.apply(8).toInt, x.apply(9), x.apply(10), x.apply(11).toInt,
          x.apply(12).toInt, x.apply(13).toInt, x.apply(14).toInt, x.apply(15).toInt,
          x.apply(16).toInt, x.apply(17).toInt, x.apply(18), x.apply(19),
          isMatchWinnerServer + "," +
          x.apply(21) + "," +
          x.apply(22) + "," +
          x.apply(23) + "," +
          x.apply(24) + "," +
          x.apply(25))

    }

    .sortBy(x => (x._1, x._2, x._3, x._4, x._5, x._7, x._8, x._12, x._13), true)

  dataset.saveAsTextFile(rootpath + "resultDataset5")
  saveOneFile(rootpath + "resultDataset5")

  val dataset = sc.textFile(rootpath + "resultDataset3.txt").map { x => x.substring(1).dropRight(1).split(",") }
    .map {
      x =>
        var isMatchWinnerServer = 0
        var isSetWinnerServer = 0
        //         (1,1,0,2005,Ai SUGIYAMA-Martina SUCHA,Martina SUCHA,0,1,1,Martina SUCHA,Ai SUGIYAMA,1,0,0,15,0,57,21,7-5 6-4,Martina SUCHA)
        val setInfo = getSetInfo(x.apply(19), x.apply(9), x.apply(10), x.apply(18), x.apply(7).toInt)
        val setWinnerName = setInfo._1
        if (x.apply(17).toUpperCase() == x.apply(9).toUpperCase())
          isMatchWinnerServer = 1
        if (setWinnerName.toUpperCase() == x.apply(9).toUpperCase())
          isSetWinnerServer = 1
        (
          x.apply(0).toInt, x.apply(1).toInt, x.apply(2).toInt, x.apply(3).toInt, x.apply(4), x.apply(5), x.apply(6).toInt, x.apply(7).toInt, x.apply(8).toInt, x.apply(9), x.apply(10), x.apply(11).toInt,
          x.apply(12).toInt, x.apply(13).toInt, x.apply(14).toInt, x.apply(15).toInt,
          x.apply(16).toInt, x.apply(17).toInt, x.apply(18), x.apply(19),
          isMatchWinnerServer + "," +
          getTotalGames(x.apply(18)) + "," +
          setWinnerName + "," +
          setInfo._2 + "," +
          isSetWinnerServer + "," +
          setInfo._3)

    }

    .sortBy(x => (x._1, x._2, x._3, x._4, x._5, x._8, x._9, x._13, x._14), true)

  dataset.saveAsTextFile(rootpath + "resultDataset4")
  saveOneFile(rootpath + "resultDataset4")
*/
  /*  var list = dataset.map { x => x._7 }.collect().toList

   list = list.tail

  val dataset2 = dataset.map { x =>
      val nextSet = list.head
     if(list.size > 1)
     list = list.tail
    (x._1, x._2, x._3, x._4, x._5, x._6, nextSet, x._8, x._9, x._10, x._11, x._12, x._13, x._14, x._15, x._16, x._17, x._18)
  }
  dataset2.saveAsTextFile(rootpath + "resultDataset3")
  saveOneFile(rootpath + "resultDataset3")
  
  val dataset = sc.textFile(rootpath + "resultDataset.txt").map { x => x.split(",") }
    .map {
      x =>
        (
          x.apply(0).toInt, x.apply(1).toInt, x.apply(2).toInt, x.apply(3).toInt, x.apply(4), x.apply(5), x.apply(6).toInt, x.apply(7).toInt, x.apply(8), x.apply(9), x.apply(10).toInt,
          x.apply(11).split("-").apply(0).toInt, x.apply(11).split("-").apply(1).toInt, x.apply(12).split("-").apply(0).toInt, x.apply(12).split("-").apply(1).toInt,
          /*x.apply(14).toInt, x.apply(15).toInt,*/ x.apply(16).toInt, x.apply(17).toInt, x.apply(18), x.apply(19), x.apply(20))
    }
    .sortBy(x => (x._1, x._2, x._3, x._4, x._5, x._7, x._8, x._12, x._13), true)

  /*  var list = dataset.map { x => x._7 }.collect().toList

  list = list.tail

 */ val dataset2 = dataset.map { x =>
    /*    val nextSet = list.head
     if(list.size > 1)
     list = list.tail
*/ val ranks = getRanks(x._6, x._9, x._10, x._16, x._17, x._18, x._19)
    (x._1, x._2, x._3, x._4, x._5, x._6, 0, x._7, x._8, x._9, x._10, x._11, x._12 + "-" + x._13, x._14 + "-" + x._15, ranks._1, ranks._2, x._20, x._6)
  }
  dataset2.saveAsTextFile(rootpath + "resultDataset2")
  saveOneFile(rootpath + "resultDataset2")
*/
  /*      
  .filter{f => f._7 != f._8}
  .map { x => 
     val score = x._13.split("-")
     var side = 2
     if(score.apply(0).toInt > score.apply(1).toInt)
       side = 1
     (x._1, x._2, x._3, x._4, x._5, x._6, x._7, side)
     }
  
  dataset2.saveAsTextFile(rootpath + "WinnerSetSide")
      saveOneFile(rootpath + "WinnerSetSide")
  val dataset3 = dataset.map { x => (x._1, x._2, x._3, x._4, x._5, x._6, x._7, x)}
  
  
  val join = dataset3.leftOuterJoin(dataset2)
  .map(f => (f._2._1, f._2._2.get))
  join.foreach(println)
  
 val dataset = sc.textFile(rootpath + "dataset_Slams_PointsWithRates.txt").map { x => x.split(",") }
 .map(x => Data(x.apply(0).toInt, x.apply(1).toInt, x.apply(2).toInt, x.apply(3), x.apply(4), x.apply(5).toInt, x.apply(6).toInt, x.apply(7), x.apply(8), x.apply(9).toInt, x.apply(10).split("-").apply(0).toInt, x.apply(10).split("-").apply(1).toInt, x.apply(11).split("-").apply(0).toInt, x.apply(11).split("-").apply(1).toInt, x.apply(12), x.apply(13).toInt, x.apply(14).toInt, x.apply(15).toInt,  x.apply(16).toInt))

 
  val dataset = sc.textFile(rootpath + "Slams_Points_New.csv").map { x => x.split(",") }.filter { x => x.apply(0) != "Tournament" }
    //  val dataset = sc.textFile(rootpath + "tennisTest.txt").map { x => x.split(",") }.filter { x => x.apply(0) != "Tournament" }
    .map { x => Data(getTournament(x.apply(0)), getSurface(x.apply(0)), getSex(x.apply(20)), getPlayers(x.apply(7), x.apply(8)), x.apply(16), x.apply(4).replaceAll("Set", "").trim().toInt, x.apply(5).toInt /*getGame(x.apply(5),x.apply(9))*/ , x.apply(7), x.apply(8), getServeNumber(x.apply(10)), 0, 0, getScore(x.apply(9))._1, getScore(x.apply(9))._2, "", 0, getPointType(x.apply(19))) }
    .sortBy(x => (x.gender, x.tournament, x.surface, x.players, x.pointWinner, x.set, x.game, x.winnerScoreAfterGame, x.opponentScoreAfterGame), true)

  var list = dataset.map { x => x.winnerScoreAfterGame + "-" + x.opponentScoreAfterGame }.collect().+:("0-0")

  val newDataset = dataset.map { x =>
    val score = getScore(list.head)
    list = list.tail
    val winnerScore = score._1
    val opponentScore = score._2
    val winner = getpointWinner(x.matchWinner, x.server, x.returner, x.serveNumber, x.winnerScoreBeforeGame, x.opponentScoreBeforeGame, winnerScore, opponentScore)
    val winnerName = winner._1
    val winnerType = winner._2
    Data(x.tournament, x.surface, x.gender, x.players, x.matchWinner, x.set, getGame(x.game, x.winnerScoreAfterGame + "-" + x.opponentScoreAfterGame), x.server, x.returner, x.serveNumber, winnerScore, opponentScore, x.winnerScoreAfterGame, x.opponentScoreAfterGame, winnerName, winnerType, x.pointType)
  }
    .filter(f => !((f.opponentScoreAfterGame == f.opponentScoreBeforeGame) && (f.winnerScoreAfterGame == f.winnerScoreBeforeGame)))

  newDataset.saveAsTextFile(rootpath + "dataset_Slams_Points_New")
  saveOneFile(rootpath + "dataset_Slams_Points_New")

  val dataset = sc.textFile(rootpath + "dataset_Slams_Points_New.txt").map { x => x.split(",") }
    .map { x => Data(x.apply(0).toInt, x.apply(1).toInt, x.apply(2).toInt, x.apply(3), x.apply(4), x.apply(5).toInt, x.apply(6).toInt, x.apply(7), x.apply(8), x.apply(9).toInt, x.apply(10).split("-").apply(0).toInt, x.apply(10).split("-").apply(1).toInt, x.apply(11).split("-").apply(0).toInt, x.apply(11).split("-").apply(1).toInt, x.apply(12), x.apply(13).toInt, x.apply(14).toInt) }
    .map { x =>
      val winner = getpointWinner(x.matchWinner, x.server, x.returner, x.winnerScoreBeforeGame, x.opponentScoreBeforeGame, x.winnerScoreAfterGame, x.opponentScoreAfterGame)

      val winnerName = winner._1
      val winnerType = winner._2
      Data(x.tournament, x.surface, x.gender, x.players, x.matchWinner, x.set, x.game, x.server, x.returner, x.serveNumber, x.winnerScoreBeforeGame, x.opponentScoreBeforeGame, x.winnerScoreAfterGame, x.opponentScoreAfterGame, winnerName, winnerType, x.pointType)
    }
  .filter(f => !f.pointWinner.isEmpty() && !f.server.isEmpty() && !f.returner.isEmpty())
  dataset.saveAsTextFile(rootpath + "dataset_Slams_Points")
  saveOneFile(rootpath + "dataset_Slams_Points")
  * 
  * */
  /*   val dataset = sc.textFile(rootpath + "dataset_Slams_PointsWithRatesNew.txt").map { x => x.split(",") }
  .map(x => Data(x.apply(0).toInt, x.apply(1).toInt, x.apply(2).toInt, x.apply(3).toInt, x.apply(4), x.apply(5), x.apply(6).toInt, x.apply(7).toInt, x.apply(8), x.apply(9), x.apply(10).toInt, x.apply(11).split("-").apply(0).toInt, x.apply(11).split("-").apply(1).toInt, x.apply(12).split("-").apply(0).toInt, x.apply(12).split("-").apply(1).toInt, x.apply(13), x.apply(14).toInt, x.apply(15).toInt, x.apply(16).toInt, x.apply(17).toInt))

  
  val tt = List(/*1, 2, 3,*/ 4)
  val yy = List(/*2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014,*/ 2015)
 
  for (t <- tt){
    for (y <- yy){
        val d0 = dataset.filter { x => (x.tournament == t && x.year == y && x.serveNumber == 0) }
       .map{x => ((x.gender, x.year, x.tournament, x.players, x.matchWinner), getPlayerWin(x.winnerScoreBeforeGame, x.opponentScoreBeforeGame, x.winnerScoreAfterGame, x.opponentScoreAfterGame, x.server, x.returner))}
       .groupByKey
       .map(f => (f._1, f._2.toList.distinct))
       .filter(s => ((s._2.size == 1) && (s._2.apply(0) != "N/A")))
       .map(f => (f._1, f._2.mkString(";")))
       
      val d1 = dataset.filter { x => (x.tournament == t && x.year == y && x.winnerScoreAfterGame == 0 && x.opponentScoreAfterGame == 0) }
       .sortBy(x => (x.gender, x.year, x.tournament, x.players, x.matchWinner, x.set, x.game), true)
       .map{x => ((x.gender, x.year, x.tournament, x.players, x.matchWinner), x.winnerScoreBeforeGame + "-" + x.opponentScoreBeforeGame)}
       .groupByKey
       .map { x => 
         var winner = 1
         val score = x._2.last.split("-")
         val s1 = score.apply(0).toInt
         val s2 = score.apply(1).toInt
         if(s2 > s1)
           winner = 2
         (x._1, winner) 
         }
       
     
      val d = dataset.filter { x => (x.tournament == t && x.year == y && x.winnerScoreAfterGame == 0 && x.opponentScoreAfterGame == 0) }
       .sortBy(x => (x.gender, x.year, x.tournament, x.players, x.matchWinner, x.set, x.game, x.winnerScoreBeforeGame, x.opponentScoreBeforeGame), true)
       .map{x => ((x.gender, x.year, x.tournament, x.players, x.matchWinner, x.set), x.winnerScoreBeforeGame + "-" + x.opponentScoreBeforeGame)}
       .groupByKey
       .map{x => ((x._1._1, x._1._2, x._1._3, x._1._4, x._1._5), (x._1._6, getScores(x._2.toList)))}
       .sortBy(x => (x._1._1, x._1._2, x._1._3, x._1._4, x._1._5, x._2._1), true)
       .groupByKey
       .map{x => (x._1, x._2.map(f => f._2).mkString(";"))}
      
       val d3 = d.leftOuterJoin(d1).leftOuterJoin(d0)
       .map(f => (f._1, f._2._1._1, f._2._1._2.getOrElse(1), f._2._2.getOrElse("0")))
        
      d3.saveAsTextFile(rootpath + "ND42-" + t + "-" + y)
      saveOneFile(rootpath + "ND42-" + t + "-" + y)
    }
  }
 */
  /*
  val dataset = sc.textFile(rootpath + "res2.txt").map { x => x.split(",") }
  .map(x => Data(x.apply(0).toInt, x.apply(1).toInt, x.apply(2).toInt, x.apply(3).toInt, x.apply(4), x.apply(5), x.apply(6).toInt, x.apply(7).toInt, x.apply(8), x.apply(9), x.apply(10).toInt, x.apply(11).split("-").apply(0).toInt, x.apply(11).split("-").apply(1).toInt, x.apply(12).split("-").apply(0).toInt, x.apply(12).split("-").apply(1).toInt, x.apply(13), x.apply(14).toInt, x.apply(15).toInt, getPlayerRate(x.apply(0).toInt, x.apply(2).toInt, x.apply(8), x.apply(18).toInt), getPlayerRate(x.apply(0).toInt, x.apply(2).toInt, x.apply(9), x.apply(19).toInt)))
  .distinct
  .sortBy(x => (x.gender, x.year, x.tournament, x.players, x.matchWinner, x.set, x.game, x.winnerScoreBeforeGame, x.opponentScoreBeforeGame), true)

  dataset.saveAsTextFile(rootpath + "dataset_Slams_PointsWithRatesNew")
  saveOneFile(rootpath + "dataset_Slams_PointsWithRatesNew")
  

  val dataset = sc.textFile(rootpath + "dataset_Slams_Points_New.txt").map { x => x.split(",") }
  .map(x => Data(x.apply(0).toInt, x.apply(1).toInt, x.apply(2).toInt, x.apply(3).toInt, x.apply(4), x.apply(5), x.apply(6).toInt, x.apply(7).toInt, x.apply(8), x.apply(9), x.apply(10).toInt, x.apply(11).split("-").apply(0).toInt, x.apply(11).split("-").apply(1).toInt, x.apply(12).split("-").apply(0).toInt, x.apply(12).split("-").apply(1).toInt, x.apply(13), x.apply(14).toInt, x.apply(15).toInt, 0, 0))
  val rates2 = sc.textFile(rootpath + "AllRankingsFullSeasons.txt").map { x => x.split(",") }
 .map{ x => ((x.apply(3).toInt, x.apply(1).toInt, x.apply(0).substring(x.apply(0).lastIndexOf(" "))), x.apply(2).toInt)}
 .distinct
    
   val rates = sc.textFile(rootpath + "AllRankingsFullSeasons.txt").map { x => x.split(",") }
 .map{ x => ((x.apply(3).toInt, x.apply(1).toInt, x.apply(0)), x.apply(2).toInt)}
 .distinct
 
   val server =  dataset.map { x => ((x.tournament, x.year, x.server), x) }.leftOuterJoin(rates)
  .map(f => (f._2._1, f._2._2.getOrElse(111)))
 
  
   val returner =  dataset.map { x => ((x.tournament, x.year, x.returner), x) }.leftOuterJoin(rates)
   .map(f => (f._2._1, f._2._2.getOrElse(111)))
 
   val server2 = server.filter(f => f._2 == 111).map{ x => ((x._1.tournament, x._1.year, x._1.server.substring(x._1.server.lastIndexOf(" "))), x._1) }
   val returner2 = returner.filter(f => f._2 == 111).map{ x => ((x._1.tournament, x._1.year, x._1.returner.substring(x._1.returner.lastIndexOf(" "))), x._1) }
   
   val server3 =  server2.leftOuterJoin(rates2)
   .map(f => (f._2._1, f._2._2.getOrElse(111)))
   
   val returner3 =  returner2.leftOuterJoin(rates2)
   .map(f => (f._2._1, f._2._2.getOrElse(111)))
  
   val sres = server.filter(f => f._2 != 111).++(server3)
   val rres = returner.filter(f => f._2 != 111).++(returner3)
  
   val stmp = sres.groupByKey.filter(f => f._2.size > 1).sortBy(x => (x._1.gender, x._1.tournament, x._1.surface, x._1.players, x._1.pointWinner, x._1.set, x._1.game, x._1.winnerScoreAfterGame, x._1.opponentScoreAfterGame), true)
   stmp.saveAsTextFile(rootpath + "serverView")
  saveOneFile(rootpath + "serverView")

    val rtmp = rres.groupByKey.filter(f => f._2.size > 1).sortBy(x => (x._1.gender, x._1.tournament, x._1.surface, x._1.players, x._1.pointWinner, x._1.set, x._1.game, x._1.winnerScoreAfterGame, x._1.opponentScoreAfterGame), true)
   rtmp.saveAsTextFile(rootpath + "retView")
  saveOneFile(rootpath + "retView")
   
  val res = sres.leftOuterJoin(rres)
  .distinct
  .sortBy(x => (x._1.gender, x._1.tournament, x._1.surface, x._1.players, x._1.pointWinner, x._1.set, x._1.game, x._1.winnerScoreAfterGame, x._1.opponentScoreAfterGame), true)
  .map(f => (f._1.toString() + "," + f._2._1 + "," +  f._2._2.getOrElse(111)))
  

  res.saveAsTextFile(rootpath + "res2")
  saveOneFile(rootpath + "res2")
*/

  /*    server.saveAsTextFile(rootpath + "server")
  saveOneFile(rootpath + "server")
    returner.saveAsTextFile(rootpath + "returner")
  saveOneFile(rootpath + "returner")
     server2.saveAsTextFile(rootpath + "server2")
  saveOneFile(rootpath + "server2")
    returner2.saveAsTextFile(rootpath + "returner2")
  saveOneFile(rootpath + "returner2")
 */

}