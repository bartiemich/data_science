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
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.mllib.stat.{ MultivariateStatisticalSummary, Statistics }
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.optimization.{ SimpleUpdater, SquaredL2Updater, L1Updater }
import org.apache.spark.sql.SQLContext;
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.Row

object TennisTest extends App {

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
    rRate: Int
    ) {
    override def toString(): String =
      tournament + "," + surface + "," + year + "," + gender + ","+ players + ","  + matchWinner + "," + set + "," + game + "," + server + "," + returner + "," + serveNumber + "," + winnerScoreBeforeGame/*.toString().replace("45", "A")*/ + "-" + opponentScoreBeforeGame/*.toString().replace("45", "A")*/  + "," + winnerScoreAfterGame/*.toString().replace("45", "A")*/ + "-" + opponentScoreAfterGame/*.toString().replace("45", "A")*/ + "," + pointWinner  + "," + isServerWin  + "," + pointType  + "," + sRate  + "," + rRate
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
    else game.toInt
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
    if(score != "0") {
     val res = score.replaceAll("A", "45").split("-").map { x => x.toInt }
    (res.apply(0), res.apply(1))
      }
    else (0, 0)
  }


  def getpointWinner(matchWinner: String, server: String, returner: String, winnerScoreBeforeGame: Int, opponentScoreBeforeGame: Int, winnerScoreAfterGame: Int, opponentScoreAfterGame: Int) = {
    val score = Math.abs(winnerScoreAfterGame - winnerScoreBeforeGame) - Math.abs(opponentScoreAfterGame - opponentScoreBeforeGame)
    if(score > 0){
      if(matchWinner == server)
        (matchWinner, 1)
        else 
          (matchWinner, 2)
    }
    else{
      if(matchWinner == server)
      (returner, 2)
      else 
       (server, 1)
    }
  }

 def saveOneFile(filename: String) = {
    FileUtil.copyMerge(
    fs, new Path(filename),
    fs, new Path(filename + ".txt"),
    true,
    fs.getConf, null)
  }
 
  def getRate(rate: RDD[(String, Int)], name: String) = {
     val set = sc.parallelize(List((name, 1)), 1)
     var rank = 100
     val r = set.leftOuterJoin(rate)
     .filter(f => !f._2._2.isEmpty)
     .map(f => f._2._2.get).first()
     rank = r
     (name, rank)
   }
   
  def getPlayerRate(name: String, rate:Int) = {
   var playerRate = rate
   if(name == "Maria Jose MARTINEZ SANCHEZ") playerRate = 45
   if(name == "Santiago GONZALEZ")   playerRate = 100
   if(name == "Rhyne WILLIAMS") playerRate = 100
   if(name == "Olivia SANCHEZ") playerRate = 100 
    
   playerRate
   }
  
  def getScoreType(scoreBefore: String, scoreAfter: String) ={
    var res = 1
    val before = scoreBefore.split("-")
    val after = scoreAfter.split("-")
    if(scoreAfter == "0-0"){
      if(before.apply(1).toInt > before.apply(0).toInt)
        res = 2
    }
    else{
      if(after.apply(1).toInt > after.apply(0).toInt)
        res = 2
    }
    res
  }
  
 def getPlayerWin(before1: Int, before2: Int, after1: Int, after2: Int, server: String, returner: String) ={
    var res = "N/A"
    if(before1 == after1 && before2 < after2){
        res = returner
    }
 
    if(before1 < after1 && before2 == after2){
        res = server
    }
    res
  }
 
def checkPlayers(p1: String, p2: String) ={
   //Jean-Rene LISNARD-Oliver MARACH,LISNARD-MARCH
  val players2 = p2.split("-")
  val players1 = p1.split("-")
  
  val contain1 = players1.exists { y => y.contains(players2.apply(0)) } 
  val contain2 = players1.exists { y => y.contains(players2.apply(1)) } 
  
  val res = contain1 && contain2
  res
  }
 
def getTotalGames(score: String) = {
    var sum = 0
    
    if (score != "0") {
      val s = score.replace("(0)", "").replace("(1)", "").replace("(2)", "").replace("(3)", "").replace("(4)", "").replace("(5)", "").replace("(6)", "").replace("(7)", "").replace("(8)", "").replace("(9)", "").replace("(10)", "").replace("(11)", "").replace("(12)", "").replace("(13)", "").replace("(14)", "").replace("(15)", "").replace("(16)", "").replace("(17)", "").replace("(18)", "").replace("(19)", "").replace("DEF", "").replace("Played", "").replace("aboned", "").replace("aboned", "").replace("and", "").replace("RET", "").replace("W/O", "").replace("-", " ").trim().split(" ")
      s.foreach { x =>
        if(!x.isEmpty())
          if(x.contains("0") ||x.contains("1") ||x.contains("2") ||x.contains("3") ||x.contains("4") ||x.contains("5") ||x.contains("6") ||x.contains("7") ||x.contains("8") ||x.contains("9") ||x.contains("10"))
        sum += x.toInt
      }
    }
    sum
  }

  val conf = new SparkConf().setAppName("TennisTest").setMaster("local")
  val sc = new SparkContext(conf)
  val rootpath = "c:\\solutioninc\\"
  val hconf = new Configuration()
  var fs = FileSystem.get(hconf);
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._
     val data = sc.textFile(rootpath + "allTennisMatches").map { x => x.split(",")}.filter { x => x.apply(0) != "tourney_id" }
   .map { x => (getPlayers(x.apply(10).toUpperCase(), x.apply(20).toUpperCase()), x.apply(2), x.apply(5), x.apply(10).toUpperCase(), x.apply(20).toUpperCase(), x.apply(27), getTotalGames(x.apply(27)))}
   .sortBy(f => (f._1, f._2, f._3))
   data.saveAsTextFile(rootpath + "allPairs")
        saveOneFile(rootpath + "allPairs")
 
        
/*  
  val dataset = sc.textFile(rootpath + "MainResultDataset.txt").map { x => x.split(",") }
  val filteredDataset = dataset.filter { x => x.apply(21) == x.apply(24) || x.apply(21) == x.apply(25) }
   
  val WinnerPointDatasetLP = filteredDataset.map{x => 
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
    x.apply(0).toDouble match{
      case 1 => ao = 1.0
      case 2 =>  fo = 1.0
      case 3 =>  uo = 1.0
      case 4 =>  w = 1.0
    }
   x.apply(1).toDouble match{
      case 1 => hard = 1.0
      case 2 =>  grass = 1.0
      case 3 =>  clay = 1.0
    }
   x.apply(2).toDouble match{
      case 0 => woman = 1.0
      case 1 =>  man = 1.0
    }
     x.apply(9).toDouble match{
      case 1 => s1 = 1.0
      case 2 =>  s2 = 1.0
      case 0 =>  df = 1.0
    }

    LabeledPoint(x.apply(27).toDouble, Vectors.dense(ao, fo, uo, w, hard, grass, clay, woman, man, x.apply(3).toDouble, x.apply(5).toDouble, x.apply(6).toDouble, s1, s2, df, x.apply(10).toDouble, x.apply(13).toDouble, x.apply(14).toDouble, x.apply(15).toDouble))
  }.toDF("label", "features")

  val test = WinnerPointDatasetLP.sample(false, 0.00004)

val lr = new LogisticRegression()
  .setMaxIter(10)
val pipeline = new Pipeline()
  .setStages(Array(lr))

val paramGrid = new ParamGridBuilder()
  .addGrid(lr.regParam, Array(0.1, 0.25, 0.5, 0.001, 0.01))
  .build()

val cv = new CrossValidator()
  .setEstimator(pipeline)
  .setEvaluator(new BinaryClassificationEvaluator)
  .setEstimatorParamMaps(paramGrid)
  .setNumFolds(2) // Use 3+ in practice

// Run cross-validation, and choose the best set of parameters.
val cvModel = cv.fit(WinnerPointDatasetLP)

// Make predictions on test documents. cvModel uses the best model found (lrModel).
cvModel.transform(test)
  .select("features", "label", "probability", "prediction")
  .collect()
  .foreach { case Row(features: Vector, label: Double, prob: Vector, prediction: Double) =>
    println(s"($features, $label) -> prob=$prob, prediction=$prediction")
  }
 */ 
/*  val data = sc.textFile(rootpath + "MainResultDataset.txt").map { x => x.split(",") }
  .map { x=> x.apply(0).toInt + "," + x.apply(1).toInt + "," +x.apply(2).toInt + "," +x.apply(3).toInt + "," +x.apply(4).toUpperCase()  + "," +x.apply(5).toInt + "," + x.apply(6).toInt + "," + x.apply(7).toUpperCase() + "," +x.apply(8).toUpperCase()  + "," +x.apply(9).toInt + "," +
          x.apply(10).replace("45", "A") + "-" + x.apply(11).replace("45", "A") + "," + x.apply(12).replace("45", "A") + "-" + x.apply(13).replace("45", "A")+ "," +
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
    
  }
  
   data.saveAsTextFile(rootpath + "MainResultDatasetA")
      saveOneFile(rootpath + "MainResultDatasetA") 
   
  
  val datasetTest = sc.textFile(rootpath + "notJoined.txt").map { x => x.substring(1).dropRight(1).split(",") }
  .map(x => ((x.apply(0).toInt, x.apply(1).toInt, x.apply(2).toInt), x.apply(3)))
  .distinct

  datasetTest.foreach(println)

  
  val datasetMatches = sc.textFile(rootpath + "AllMatchesDetailed.txt").map { x => x.substring(1).dropRight(1).split(",") }
  .map{x => 
    ((x.apply(0).toInt, x.apply(3).toInt, x.apply(2).toInt), x.mkString(","))}
  .distinct 
  
  datasetMatches.foreach(println)
  
  
  val data = datasetTest.leftOuterJoin(datasetMatches)
  data.saveAsTextFile(rootpath + "join22")
      saveOneFile(rootpath + "join22")
 

  
  val datasetMatches = sc.textFile(rootpath + "AllMatchesDetailed.txt").map { x => x.substring(1).dropRight(1).split(",") }
  .map{x => 
    ((x.apply(0).toInt, x.apply(3).toInt, x.apply(2).toInt, x.apply(4)), (x.apply(7), x.apply(8), x.reverse.dropRight(9).reverse.mkString(";")))}
  .distinct 
*/   

  
 /*
  val dataAdd =  sc.textFile(rootpath + "toAdd.txt").map { x => x.substring(1).dropRight(1).split(",") }
  .map { x => x.mkString(",")}
 
 
   dataAdd.saveAsTextFile(rootpath + "dataAdd")
      saveOneFile(rootpath + "dataAdd")
 */     
//  val join = datasetMatches.leftOuterJoin(dataset).filter(f => f._2._2.isEmpty).map(f => ((f._1._1, f._1._2, f._1._3), (f._1._4, f._2._1)))
//  .map(f => (f._2._1, f._2._2.getOrElse("0")))
  
//  join.filter(f => f._2 != "0").saveAsTextFile(rootpath + "join")
//      saveOneFile(rootpath + "join")
  
  
 /*     
  val datasetTest = sc.textFile(rootpath + "notJoined.txt").map { x => x.substring(1).dropRight(1).split(",") }
  .map(x => ((x.apply(0).toInt, x.apply(1).toInt, x.apply(2).toInt), x.apply(3)))
  .distinct

  val data = datasetTest.leftOuterJoin(join)
  .filter(f => checkPlayers(f._2._1, f._2._2.get._1))
  .map { x => (x._1._1, x._1._2, x._1._3, x._2._1, x._2._2.get._1, x._2._2.get._2._1, x._2._2.get._2._2, x._2._2.get._2._3) }
  .sortBy(f => (f._1, f._2, f._3, f._4, f._5))

  
  val dataset2 = sc.textFile(rootpath + "join22222.txt").map { x => x.substring(1).dropRight(1).split(",") }
   .map(x => ((x.apply(0).toInt, x.apply(1).toInt, x.apply(2).toInt, x.apply(3)), (x.apply(5), x.apply(6), x.apply(7))))
  
  println(dataset2.first())
   println(dataset.first())
   val join = dataset.leftOuterJoin(dataset2).filter(f => !f._2._2.isEmpty)
   .map(f => (f._2._1, f._2._2.get))
 */  

/*  val dataset2 = sc.textFile(rootpath + "join22222.txt").map { x => x.substring(1).dropRight(1).split(",") }

  val count = dataset2.count().toDouble / dataset.count().toDouble * 100    
  
  println(count)
*/
  /*
  val datasetWomen = sc.textFile(rootpath + "women").map { x => x.split(";") }
  .filter { x => x.apply(2) == "Wimbledon" || x.apply(2) == "US Open" || x.apply(2) == "French Open" || x.apply(2) == "Australian Open" }
  .map { x =>  
    var i1 = x.apply(9).size
    var i2 = x.apply(10).size
    if(x.apply(9).indexOf(" ") != -1)
      i1 = x.apply(9).indexOf(" ")
    if(x.apply(10).indexOf(" ") != -1)
      i2 = x.apply(10).indexOf(" ")
      
    (getTournament(x.apply(2)), getSurface(x.apply(2)), 0, x.apply(3).substring(6).toInt, x.apply(9).substring(0, i1).toUpperCase(), x.apply(10).substring(0, i2).toUpperCase(), x.apply(11), x.apply(12), x.apply(15), x.apply(16), x.apply(17), x.apply(18), x.apply(19), x.apply(20))}
  datasetWomen.saveAsTextFile(rootpath + "WomenMatches")
      saveOneFile(rootpath + "WomenMatches")

    val datasetMen = sc.textFile(rootpath + "men").map { x => x.split(";") }
  .filter { x => x.apply(2) == "Wimbledon" || x.apply(2) == "US Open" || x.apply(2) == "French Open" || x.apply(2) == "Australian Open" }
  .map { x =>  
    var i1 = x.apply(9).size
    var i2 = x.apply(10).size
    if(x.apply(9).indexOf(" ") != -1)
      i1 = x.apply(9).indexOf(" ")
    if(x.apply(10).indexOf(" ") != -1)
      i2 = x.apply(10).indexOf(" ")
      
    (getTournament(x.apply(2)), getSurface(x.apply(2)), 1, x.apply(3).substring(6).toInt, x.apply(9).substring(0, i1).toUpperCase(), x.apply(10).substring(0, i2).toUpperCase(), x.apply(11), x.apply(12), x.apply(15), x.apply(16), x.apply(17), x.apply(18), x.apply(19), x.apply(20), x.apply(21), x.apply(22), x.apply(23), x.apply(24))}
  datasetMen.saveAsTextFile(rootpath + "MenMatches")
      saveOneFile(rootpath + "MenMatches")
*/ 
  
  
 /*
  val dataset = sc.textFile(rootpath + "Slams_Sets_New.csv").map { x => x.split(",") }
  .filter { x => x.apply(16) == "Won" }
   .map { x => 
     var isMatchWinnerWinSet = 1
     if(!x.apply(18).isEmpty() && !x.apply(19).isEmpty()){
     val p1 = x.apply(18).toInt
     val p2 = x.apply(19).toInt
     if(p2 > p1)
       isMatchWinnerWinSet = 0
     }
     else
        isMatchWinnerWinSet = 2
     ((getTournament(x.apply(0)), getSurface(x.apply(0)), getSex(x.apply(12)), x.apply(1).toInt, x.apply(2), x.apply(4).replaceAll("Set", "").trim().toInt, getPlayers(x.apply(9), x.apply(17))), isMatchWinnerWinSet) }
 
 val dataset2 = sc.textFile(rootpath + "Slams_Sets_New.csv").map { x => x.split(",") }
  .filter { x => x.apply(16) == "Lost" }
   .map { x => 
     var isMatchWinnerWinSet = 0
     if(!x.apply(18).isEmpty() && !x.apply(19).isEmpty()){
     val p1 = x.apply(18).toInt
     val p2 = x.apply(19).toInt
     if(p2 > p1)
       isMatchWinnerWinSet = 1
     }
     else
        isMatchWinnerWinSet = 2
     ((getTournament(x.apply(0)), getSurface(x.apply(0)), getSex(x.apply(12)), x.apply(1).toInt, x.apply(2), x.apply(4).replaceAll("Set", "").trim().toInt, getPlayers(x.apply(9), x.apply(17))), isMatchWinnerWinSet) }

   val res = dataset.leftOuterJoin(dataset2)
   .map(f => (f._1, f._2._1, f._2._2.getOrElse(2)))
   .sortBy(f => (f._1._4), true)
   res.saveAsTextFile(rootpath + "setWinners")
      saveOneFile(rootpath + "setWinners")

   res.filter(f => (f._2 != f._3) || (f._2 == 2 && f._3 == 2)).sortBy(f => (f._1._4), true).saveAsTextFile(rootpath + "reserrors")
      saveOneFile(rootpath + "reserrors")
*/
      
 /* 
   val dataset = sc.textFile(rootpath + "WomenMatches.txt").map { x => x.substring(1).dropRight(1).split(",") }
    .map { x =>
      var set1 = 1
      var set2 = 1
      var set3 = 1
      var score1 = ""
      var score2 = ""
      var score3 = ""
      
      if ((x.size > 9) && (!x.apply(8).isEmpty() && !x.apply(9).isEmpty())) {
        score1 = x.apply(8) + "-" + x.apply(9)
        if (x.apply(8).toInt < x.apply(9).toInt) {
          set1 = 0
        }
      }
      if ((x.size > 11) && (!x.apply(10).isEmpty() && !x.apply(11).isEmpty())) {

        score2 = x.apply(10) + "-" + x.apply(11)
        if (x.apply(10).toInt < x.apply(11).toInt) {
          set2 = 0
        }
      }
      if ((x.size > 13) && (!x.apply(12).isEmpty() && !x.apply(13).isEmpty())) {
        score3 = x.apply(12) + "-" + x.apply(13)
        if (x.apply(12).toInt < x.apply(13).toInt) {
          set3 = 0
        }
      }
   
      (x.apply(0), x.apply(1), x.apply(2), x.apply(3), getPlayers(x.apply(4), x.apply(5)), x.apply(4), x.apply(5), x.apply(6), x.apply(7), (score1, set1), (score2, set2), (score3, set3))
    }

    dataset.saveAsTextFile(rootpath + "WomensMatchesDetailed")
      saveOneFile(rootpath + "WomensMatchesDetailed")
   
    
      val dataset2 = sc.textFile(rootpath + "MenMatches.txt").map { x => x.substring(1).dropRight(1).split(",") }
    .map { x =>
      var set1 = 1
      var set2 = 1
      var set3 = 1
      var score5 = ""
      var set5 = 1
      var score1 = ""
      var score2 = ""
      var score3 = ""
      var set4 = 1
      var score4 = ""
      
      if ((x.size > 9) && (!x.apply(8).isEmpty() && !x.apply(9).isEmpty())) {
        score1 = x.apply(8) + "-" + x.apply(9)
        if (x.apply(8).toInt < x.apply(9).toInt) {
          set1 = 0
        }
      }
      if ((x.size > 11) && (!x.apply(10).isEmpty() && !x.apply(11).isEmpty())) {

        score2 = x.apply(10) + "-" + x.apply(11)
        if (x.apply(10).toInt < x.apply(11).toInt) {
          set2 = 0
        }
      }
      if ((x.size > 13) && (!x.apply(12).isEmpty() && !x.apply(13).isEmpty())) {
        score3 = x.apply(12) + "-" + x.apply(13)
        if (x.apply(12).toInt < x.apply(13).toInt) {
          set3 = 0
        }
      }
      if ((x.size > 15) && (!x.apply(14).isEmpty() && !x.apply(15).isEmpty())) {

        score4 = x.apply(14) + "-" + x.apply(15)
        if (x.apply(14).toInt < x.apply(15).toInt) {
          set4 = 0
        }
      }
      if ((x.size > 17) && (!x.apply(16).isEmpty() && !x.apply(17).isEmpty())) {
        score5 = x.apply(16) + "-" + x.apply(17)
        if (x.apply(16).toInt < x.apply(17).toInt) {
          set5 = 0
        }
      }
   
      (x.apply(0), x.apply(1), x.apply(2), x.apply(3), getPlayers(x.apply(4), x.apply(5)), x.apply(4), x.apply(5), x.apply(6), x.apply(7), (score1, set1), (score2, set2), (score3, set3), (score4, set4), (score5, set5))
    }
    dataset2.saveAsTextFile(rootpath + "MensMatchesDetailed")
      saveOneFile(rootpath + "MensMatchesDetailed")
*/ 

   
/*
    val dataset = sc.textFile(rootpath + "dataset_Slams_PointsWithRatesNew.txt").map { x => x.split(",") }
  .map(x => Data(x.apply(0).toInt, x.apply(1).toInt, x.apply(2).toInt, x.apply(3).toInt, x.apply(4), x.apply(5), x.apply(6).toInt, x.apply(7).toInt, x.apply(8), x.apply(9), x.apply(10).toInt, x.apply(11).split("-").apply(0).toInt, x.apply(11).split("-").apply(1).toInt, x.apply(12).split("-").apply(0).toInt, x.apply(12).split("-").apply(1).toInt, x.apply(13), x.apply(14).toInt, x.apply(15).toInt, x.apply(16).toInt, x.apply(17).toInt))

  
  val tt = List(/*1, 2, 3,*/ 4)
  val yy = List(/*2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014,*/ 2015)
 
  for (t <- tt){
    for (y <- yy){
    
      val d = dataset.filter { x => (x.tournament == t && x.year == y && x.serveNumber == 0) }
 //      .sortBy(x => (x.gender, x.year, x.tournament, x.players, x.matchWinner, x.set, x.game, x.winnerScoreBeforeGame, x.opponentScoreBeforeGame), true)
       .map{x => ((x.gender, x.year, x.tournament, x.players, x.matchWinner), getPlayerWin(x.winnerScoreBeforeGame, x.opponentScoreBeforeGame, x.winnerScoreAfterGame, x.opponentScoreAfterGame, x.server, x.returner))}
       .groupByKey
       .map(f => (f._1, f._2.toList.distinct))
       .filter(s => ((s._2.size == 1) && (s._2.apply(0) != "N/A")))
       
      d.saveAsTextFile(rootpath + "N2fault1-" + t + "-" + y)
      saveOneFile(rootpath + "N2fault1-" + t + "-" + y)
    }
  }

 
   val dataset = sc.textFile(rootpath + "ND").map { x => x.split(",") }
  .sortBy(f => (f.apply(0), f.apply(1), f.apply(2)), true)
  .map{x => x.mkString(",")}
  dataset.saveAsTextFile(rootpath + "TennisResults")
  saveOneFile(rootpath + "TennisResults")
  
 val dataset = sc.textFile(rootpath + "AllRankingsFullSeasons.txt").map { x => x.split(",") }
 .map{ x => ((x.apply(3), x.apply(1), x.apply(0)), x.apply(2))}
 .distinct

  dataset.saveAsTextFile(rootpath + "WomensNew")
  saveOneFile(rootpath + "WomensNew")
 
 
  val dataset = sc.textFile(rootpath + "dataset_Slams_Points_New.txt").map { x => x.split(",") }
  .map{ x => (x.apply(0), x.apply(1), x.apply(2), x.apply(3), x.apply(4), x.apply(5), x.apply(8), x.apply(9), x.apply(10), x.apply(11), x.apply(12), x.apply(15) )}
  .filter(f => f._12.toInt == 4)
  .distinct()
  
  val d1 = dataset.filter(f => (f._11 == "0-0"))
  .map{f => ((f._1, f._2, f._3, f._4, f._5), getScoreType(f._10, f._11))}
  .groupByKey
  .filter(f => f._2.toList.distinct.size == 1)
  .map(f => (f._1, f._2.toList.distinct.apply(0)))

  val d2 = dataset.filter(f => (f._11 != "0-0"))
  .map{f => ((f._1, f._2, f._3, f._4, f._5), getScoreType(f._10, f._11))}
  .groupByKey
  .filter(f => f._2.toList.distinct.size == 1)
  .map(f => (f._1, f._2.toList.distinct.apply(0)))

  
  val d3 = d2.leftOuterJoin(d1)
  .filter(f => f._2._2.isEmpty)
  .map(f => (f._1, f._2._1))
  
  datasetFull.leftOuterJoin(d1.++(d3).distinct())
  .map(f => (f._2._1, f._2._2.getOrElse(0)))
  .sortBy(x => (x._1.gender, x._1.year, x._1.tournament, x._1.players, x._1.pointWinner, x._1.set, x._1.game, x._1.winnerScoreBeforeGame, x._1.opponentScoreBeforeGame), true)
  .saveAsTextFile(rootpath + "dataset_Slams_Points_New3")
  saveOneFile(rootpath + "dataset_Slams_Points_New3")
  
  
        val dataset = sc.textFile(rootpath + "Slams_Points_New.csv").map { x => x.split(",") }.filter { x => x.apply(0) != "Tournament" }
//  val dataset = sc.textFile(rootpath + "Slams_Points_sample.txt").map { x => x.split(",") }.filter { x => x.apply(0) != "Tournament" }
    .map { x => Data(getTournament(x.apply(0)), getSurface(x.apply(0)), x.apply(1).toInt,  getSex(x.apply(20)), getPlayers(x.apply(7), x.apply(8)), x.apply(16), x.apply(4).replaceAll("Set", "").trim().toInt, x.apply(5).toInt /*getGame(x.apply(5),x.apply(9))*/, x.apply(7), x.apply(8), getServeNumber(x.apply(10)), 0, 0, getScore(x.apply(9))._1, getScore(x.apply(9))._2, "", 0, getPointType(x.apply(19)), 0, 0) }
    .filter(f => !f.matchWinner.isEmpty() && !f.server.isEmpty() && !f.returner.isEmpty())
    .sortBy(x => (x.gender, x.year, x.tournament, x.players, x.pointWinner, x.set, x.game, x.winnerScoreAfterGame, x.opponentScoreAfterGame), true)

 var list = dataset.map { x => x.winnerScoreAfterGame + "-" + x.opponentScoreAfterGame }.collect().+:("0-0")

  val newDataset = dataset.map { x =>
    val score = getScore(list.head)
    list = list.tail
    val Score1 = score._1
    val Score2 = score._2
    Data(x.tournament, x.surface, x.gender, x.year, x.players, x.matchWinner, x.set, getGame(x.game, x.winnerScoreAfterGame + "-" + x.opponentScoreAfterGame), x.server, x.returner, x.serveNumber, Score1, Score2, x.winnerScoreAfterGame, x.opponentScoreAfterGame, "", 0, x.pointType, 0, 0)
  }
    .filter(f => !((f.opponentScoreAfterGame == f.opponentScoreBeforeGame) && (f.winnerScoreAfterGame == f.winnerScoreBeforeGame)))

 
  newDataset.saveAsTextFile(rootpath + "dataset_Slams_Points_New")
  saveOneFile(rootpath + "dataset_Slams_Points_New")
*/
    
 /*
 val dataset = sc.textFile(rootpath + "dataset_Slams_Points.txt").map { x => x.split(",") }
 .map(x => Data(x.apply(0).toInt, x.apply(1).toInt, x.apply(2).toInt, x.apply(3), x.apply(4), x.apply(5).toInt, x.apply(6).toInt, x.apply(7), x.apply(8), x.apply(9).toInt, x.apply(10).split("-").apply(0).toInt, x.apply(10).split("-").apply(1).toInt, x.apply(11).split("-").apply(0).toInt, x.apply(11).split("-").apply(1).toInt, x.apply(12), x.apply(13).toInt, x.apply(14).toInt))
 
 dataset.saveAsTextFile(rootpath + "dataset_Slams_PointsA")
 saveOneFile(rootpath + "dataset_Slams_PointsA")
 */
 
/*   
  val datasetLP = sc.textFile(rootpath + "dataset_Slams_PointsWithRates.txt").map { x => x.split(",") }
  .map(x => LabeledPoint(x.apply(13).toDouble, Vectors.dense(x.apply(0).toDouble, x.apply(1).toDouble, x.apply(2).toDouble, x.apply(5).toDouble, x.apply(6).toDouble, x.apply(9).toDouble, x.apply(10).split("-").apply(0).toDouble, x.apply(10).split("-").apply(1).toDouble, x.apply(14).toDouble, x.apply(15).toDouble, x.apply(16).toDouble))) 
  
val model = NaiveBayes.train(datasetLP, lambda = 1.0, modelType = "multinomial")
//println("weights: %s, intercept: %s".format(model..weights, model.intercept)) //weights: [0.18940064233163645,0.07173524892945958,0.049635610845752944,0.08361438719596898,0.11521988277711567,0.07428518097477223,1.3922404925481389,-1.0186671933075253,-0.002156673263756261,0.6022886822787612,1.4256324980277655], intercept: 0.0

  val result = datasetLP.map { x => (x.label, model.predict(x.features) ) }
//  result.saveAsTextFile(rootpath + "resultPredictFull")
//  saveOneFile(rootpath + "resultPredictFull")
  
  val percentage1 = result.filter(f => f._1 == f._2).count().toDouble / datasetLP.count().toDouble * 100
  println("prediction is right in " + percentage1 + " percentages")
*/
 

/* 
 val menRate = sc.textFile(rootpath + "MensRankings.txt").map { x => x.split(",") }
 .map(x => (x.apply(0), (x.apply(1), x.apply(2))))
 .groupByKey
 .sortBy(s => s._1)
 .map(x => (x._1 + "," +  (x._2.map(f => f._2.toDouble).sum / x._2.size).intValue()))
 menRate.saveAsTextFile(rootpath + "mensRateNew")
  saveOneFile(rootpath + "mensRateNew")
 
 val womenRate = sc.textFile(rootpath + "WomensRankings.txt").map { x => x.split(",") }
 .map(x => (x.apply(0), (x.apply(1), x.apply(2))))
 .groupByKey
 .sortBy(s => s._1)
 .map(x => (x._1 + "," + (x._2.map(f => f._2.toDouble).sum / x._2.size).intValue()))
 womenRate.saveAsTextFile(rootpath + "womensRateNew")
  saveOneFile(rootpath + "womensRateNew")
 */
 
 /* не подходит!!!!!!!!
    val updater = new SquaredL2Updater()
    val algorithm = new LinearRegressionWithSGD()
    algorithm.optimizer
      .setNumIterations(100)
      .setStepSize(0.1)
      .setUpdater(updater)
      .setRegParam(0.01)
  val model = algorithm.run(dataset)
    println("weights: %s, intercept: %s".format(model.weights, model.intercept)) 
    //weights: [-0.019211874101626128,-0.06678487931536611,-0.0010071229090883906,-0.07441894504653024,-0.1350680826943308,-0.029700338007492253,-0.03933055708582911,-0.8600739548443065,-0.006870600035348286], intercept: 0.0

 val result = sc.parallelize(dataset.take(100), 1).map { x => (x.label, model.predict(x.features) ) }
  result.saveAsTextFile(rootpath + "resultPredict")
  saveOneFile(rootpath + "resultPredict")
 */
/* 
  val model = new LogisticRegressionWithSGD().run(datasetLP)
  println("weights: %s, intercept: %s".format(model.weights, model.intercept)) //weights: [-0.019211874101626128,-0.06678487931536611,-0.0010071229090883906,-0.07441894504653024,-0.1350680826943308,-0.029700338007492253,-0.03933055708582911,-0.8600739548443065,-0.006870600035348286], intercept: 0.0

  val y = datasetLP.first()
  val p = model.predict(y.features)
*/
 /* 
  val result = sc.parallelize(dataset.take(100), 1).map { x => (x.label, model.predict(x.features) ) }
  result.saveAsTextFile(rootpath + "resultPredict")
  saveOneFile(rootpath + "resultPredict")
  
  val percentage1 = result.filter(f => f._1 == f._2).count().toDouble / dataset.count().toDouble * 100
  println("prediction is right in " + percentage1 + " percentages")
  





























  
    .map { x => ((x.apply(0), x.apply(1), x.apply(2), x.apply(3), x.apply(4), getGame(x.apply(5),x.apply(9))), x/*(x.apply(9), x.apply(19))*/) }
    
  val unknown = dataset.filter(f => (f._2.apply(9) == "0-0") && (f._2.apply(19) == "Unknown")).map(f => (f._1, 1))
  val break = dataset.filter(f => (f._2.apply(9) == "0-0") && (f._2.apply(19) != "Unknown")).map(f => (f._1, 1))
  
  val d1 = unknown.leftOuterJoin(dataset).filter(f => f._2._1 == 1)
  .map(x => (x._2._2.get.apply(0),x._2._2.get.apply(1),x._2._2.get.apply(2),x._2._2.get.apply(3),x._2._2.get.apply(4),getGame(x._2._2.get.apply(5),x._2._2.get.apply(9)),x._2._2.get.apply(6),x._2._2.get.apply(7),x._2._2.get.apply(8),x._2._2.get.apply(9),x._2._2.get.apply(10),x._2._2.get.apply(11),x._2._2.get.apply(12),x._2._2.get.apply(13),x._2._2.get.apply(14),x._2._2.get.apply(15),x._2._2.get.apply(16),x._2._2.get.apply(17),x._2._2.get.apply(18),x._2._2.get.apply(19),x._2._2.get.apply(20)))
  .distinct
  .sortBy(x => (x._1, x._2, x._3, x._4, x._7.toInt), true)
  .map(x => x._1 + "," + x._2 + "," + x._3 + "," + x._4 + "," + x._5 + "," + x._6 + "," + x._7 + "," + x._8 + "," + x._9 + "," + x._10 + "," + x._11 + "," + x._12 + "," + x._13 + "," + x._14 + "," + x._15 + "," + x._16 + "," + x._17 + "," + x._18 + "," + x._19 + "," + x._20 + "," + x._21)

  
  val d2 = break.leftOuterJoin(dataset).filter(f => f._2._1 == 1)
  .map(x => (x._2._2.get.apply(0),x._2._2.get.apply(1),x._2._2.get.apply(2),x._2._2.get.apply(3),x._2._2.get.apply(4),getGame(x._2._2.get.apply(5),x._2._2.get.apply(9)),x._2._2.get.apply(6),x._2._2.get.apply(7),x._2._2.get.apply(8),x._2._2.get.apply(9),x._2._2.get.apply(10),x._2._2.get.apply(11),x._2._2.get.apply(12),x._2._2.get.apply(13),x._2._2.get.apply(14),x._2._2.get.apply(15),x._2._2.get.apply(16),x._2._2.get.apply(17),x._2._2.get.apply(18),x._2._2.get.apply(19),x._2._2.get.apply(20)))
  .distinct
  .sortBy(x => (x._1, x._2, x._3, x._4, x._7.toInt), true)
  .map(x => x._1 + "," + x._2 + "," + x._3 + "," + x._4 + "," + x._5 + "," + x._6 + "," + x._7 + "," + x._8 + "," + x._9 + "," + x._10 + "," + x._11 + "," + x._12 + "," + x._13 + "," + x._14 + "," + x._15 + "," + x._16 + "," + x._17 + "," + x._18 + "," + x._19 + "," + x._20 + "," + x._21)
 
    d1.saveAsTextFile(rootpath + "Slams_Points_New_Part1")
    saveOneFile(rootpath + "Slams_Points_New_Part1")
  
    d2.saveAsTextFile(rootpath + "Slams_Points_New_Part2")
    saveOneFile(rootpath + "Slams_Points_New_Part2")
    * 
    */
      
 /* 
    val dataset = sc.textFile(rootpath + "dataset3Add.txt").map { x => x.substring(1).dropRight(1).split(",") }
    .map { x => ((x.apply(0).toInt,x.apply(2).toInt,x.apply(3).toInt,x.apply(4).toUpperCase()),
    x.apply(0) + "," +  x.apply(1) + "," +  x.apply(2) + "," +  x.apply(3) + "," +  x.apply(4) + "," +  x.apply(5) + "," +  x.apply(6) + "," +  x.apply(7) + "," +  x.apply(8) + "," +  x.apply(9) + "," +  x.apply(10) + "," +  x.apply(11) + "," +  x.apply(12) + "," +  x.apply(13) + "," +  x.apply(14) + "," +  x.apply(15) + "," +  x.apply(16) + "," +  x.apply(17) )  }
    .distinct()

  val datasetM = sc.textFile(rootpath + "tennisM").map { x => x.split(",") }.filter { x => x.apply(0) != "tourney_id" }
  .filter { x => getTournament(x.apply(1)) == 1 ||  getTournament(x.apply(1)) == 2 ||  getTournament(x.apply(1)) == 3 ||  getTournament(x.apply(1)) == 4}
  .map { x => ((getTournament(x.apply(1)), 1, x.apply(5).substring(0, 4).toInt, getPlayers(x.apply(10), x.apply(20))), (x.apply(10), x.apply(20), x.apply(25), x.apply(26), x.apply(27))) }
 
  val datasetW = sc.textFile(rootpath + "tennisW").map { x => x.split(",") }.filter { x => x.apply(0) != "tourney_id" }
  .filter { x => getTournament(x.apply(1)) == 1 ||  getTournament(x.apply(1)) == 2 ||  getTournament(x.apply(1)) == 3 ||  getTournament(x.apply(1)) == 4}
  .map { x => ((getTournament(x.apply(1)), 0, x.apply(5).substring(0, 4).toInt, getPlayers(x.apply(10), x.apply(20))), (x.apply(10), x.apply(20), x.apply(25), x.apply(26), x.apply(27))) }
 val dataset2 = datasetM.++(datasetW)

 val join = dataset.leftOuterJoin(dataset2)
 .filter(f => f._2._2.isDefined)
 .map(f => (f._2._1, f._2._2.get._3, f._2._2.get._4, f._2._2.get._5))
// .map(f => (f._2._2.get, f._2._1._3, f._2._1._4, f._2._1._5))
 
  join.saveAsTextFile(rootpath + "joinRest2")
      saveOneFile(rootpath + "joinRest2")
  
  val dataAll =  sc.textFile(rootpath + "dataset3Add.txt").map { x => x.split(",") }   
      
    */  
      
      
      
/*   val dataset2 = sc.textFile(rootpath + "dataset_Slams_PointsWithRatesNew.txt").map { x => x.split(",").mkString(",") }
  
   val dataset3 = dataset2.subtract(dataset).map { x => "(" + x + ",0,0,0)" }.sortBy(x => (x),true)
  
   dataset3.saveAsTextFile(rootpath + "dataset3Add")
      saveOneFile(rootpath + "dataset3Add")

   val dataset = sc.textFile(rootpath + "subs.txt").map { x => x.substring(1).dropRight(1).split(",") }
   .map { x => ((x.apply(0).toInt, x.apply(2).toInt, x.apply(3).toInt,x.apply(4).toUpperCase()),1) }
   
    val datasetM = sc.textFile(rootpath + "tennisM").map { x => x.split(",") }.filter { x => x.apply(0) != "tourney_id" }
  .filter { x => getTournament(x.apply(1)) == 1 ||  getTournament(x.apply(1)) == 2 ||  getTournament(x.apply(1)) == 3 ||  getTournament(x.apply(1)) == 4}
  .map { x => ((getTournament(x.apply(1)), 0, x.apply(5).substring(0, 4).toInt, getPlayers(x.apply(10), x.apply(20))), (x.apply(10), x.apply(20), x.apply(25), x.apply(26), x.apply(27))) }
 
  val datasetW = sc.textFile(rootpath + "tennisW").map { x => x.split(",") }.filter { x => x.apply(0) != "tourney_id" }
  .filter { x => getTournament(x.apply(1)) == 1 ||  getTournament(x.apply(1)) == 2 ||  getTournament(x.apply(1)) == 3 ||  getTournament(x.apply(1)) == 4}
  .map { x => ((getTournament(x.apply(1)), 0, x.apply(5).substring(0, 4).toInt, getPlayers(x.apply(10), x.apply(20))), (x.apply(10), x.apply(20), x.apply(25), x.apply(26), x.apply(27))) }

 val dataset2 = datasetM.++(datasetW)

 val join = dataset2.leftOuterJoin(dataset)
 .filter(f => f._2._2.isDefined)
 .map(f => ((f._1._1, f._1._2, f._1._3, f._1._4), f._2._1))
// .map(f => (f._2._2.get, f._2._1._3, f._2._1._4, f._2._1._5))
 
  join.saveAsTextFile(rootpath + "joinRest")
      saveOneFile(rootpath + "joinRest")
 
   val dataset3 = sc.textFile(rootpath + "dataset_Slams_PointsWithRatesNew.txt").map { x => x.split(",") }
   .map { x => ((x.apply(0).toInt, x.apply(2).toInt, x.apply(3).toInt,x.apply(4).toUpperCase()), x.mkString(",")) }
 val join2 = join.leftOuterJoin(dataset3)
 .filter(f => f._2._2.isDefined)
 .map(f => (f._2._2.get, f._2._1._3, f._2._1._4, f._2._1._5))

 
  join2.saveAsTextFile(rootpath + "RestData")
      saveOneFile(rootpath + "RestData")
  
  val dataset = sc.textFile(rootpath + "dataset_Slams_PointsWithRatesNew.txt").map { x => x.split(",") }
   .map { x => ((x.apply(0).toInt, x.apply(1).toInt, x.apply(2).toInt, x.apply(3).toInt,x.apply(4).toUpperCase())) }
.distinct()
  val dataset2 = sc.textFile(rootpath + "dataset_Slams_PointsWithRatesNewDetailed.txt").map { x => x.substring(1).dropRight(1).split(",") }
   .map { x => ((x.apply(0).toInt, x.apply(1).toInt, x.apply(3).toInt, x.apply(2).toInt,x.apply(4).toUpperCase())) }
.distinct()
   val sub = dataset.subtract(dataset2).distinct()
   sub.saveAsTextFile(rootpath + "subs.txt")
      saveOneFile(rootpath + "subs.txt")
 
      */
/*
  val dataset = sc.textFile(rootpath + "dataset_Slams_PointsWithRatesNew.txt").map { x => x.split(",") }
   .map { x => ((x.apply(0).toInt, x.apply(2).toInt, x.apply(3).toInt,x.apply(4).toUpperCase()), x.mkString(",")) }
*/
 /*   .distinct
    .sortBy(x => (x._1, x._2, x._3, x._4), true)
   var i = 0
   var list = dataset.map { x => x._4 }.collect().+:(0).toList
   
   
   val dataset4 = dataset.map { x => 
     val count = list.head
     list = list.tail
     if(count < x._4)
       i = i.+(1)
       else
       i = 1
     ((x._1, x._2, x._3, x._5), i) 
     }
 
   
     val dataset5 = dataset.map { x => 
     val count = list.head
     list = list.tail
     if(count < x._4)
       i = i.+(1)
       else
       i = 1
     ((x._1, x._2, x._3, x._5), 1) 
     }
*//*
   
 */ /*    
  val datasetM = sc.textFile(rootpath + "tennisM").map { x => x.split(",") }.filter { x => x.apply(0) != "tourney_id" }
  .filter { x => getTournament(x.apply(1)) == 1 ||  getTournament(x.apply(1)) == 2 ||  getTournament(x.apply(1)) == 3 ||  getTournament(x.apply(1)) == 4}
  .map { x => ((getTournament(x.apply(1)), 0, x.apply(5).substring(0, 4).toInt, getPlayers(x.apply(10), x.apply(20))), (x.apply(10), x.apply(20), x.apply(25), x.apply(26), x.apply(27))) }
 
  val datasetW = sc.textFile(rootpath + "tennisW").map { x => x.split(",") }.filter { x => x.apply(0) != "tourney_id" }
  .filter { x => getTournament(x.apply(1)) == 1 ||  getTournament(x.apply(1)) == 2 ||  getTournament(x.apply(1)) == 3 ||  getTournament(x.apply(1)) == 4}
  .map { x => ((getTournament(x.apply(1)), 0, x.apply(5).substring(0, 4).toInt, getPlayers(x.apply(10), x.apply(20))), (x.apply(10), x.apply(20), x.apply(25), x.apply(26), x.apply(27))) }

 val dataset2 = datasetM.++(datasetW)

 val join = dataset2.leftOuterJoin(dataset)
 .filter(f => f._2._2.isDefined)
 .map(f => (f._2._2.get, f._2._1._3, f._2._1._4, f._2._1._5))
 
  join.saveAsTextFile(rootpath + "joinW20052006_2.txt")
      saveOneFile(rootpath + "joinW20052006_2.txt")
*/
 

/*
 val datasetM = sc.textFile(rootpath + "tennisM").map { x => x.split(",") }.filter { x => x.apply(0) != "tourney_id" }
  .map { x => ((getTournament(x.apply(1)), 1, x.apply(5).substring(0, 4).toInt, x.apply(6).toInt), (x.apply(10), x.apply(20), x.apply(25), x.apply(26), x.apply(27))) }
 
  val datasetW = sc.textFile(rootpath + "tennisW").map { x => x.split(",") }.filter { x => x.apply(0) != "tourney_id" }
  .map { x => ((getTournament(x.apply(1)), 0, x.apply(5).substring(0, 4).toInt, x.apply(6).toInt), (x.apply(10), x.apply(20), x.apply(25), x.apply(26), x.apply(27))) }

  
  
 val dataset2 = datasetM.++(datasetW)
 val dataset3 = sc.textFile(rootpath + "dataset_Slams_PointsWithRatesNew.txt").map { x => x.split(",") }
 .filter { x => x.apply(3).toInt == 2005 }
 .map { x => ((x.apply(0).toInt, x.apply(2).toInt, x.apply(3).toInt, x.apply(4)), x) }

 val join = dataset3.leftOuterJoin(dataset4)
  .filter(f => f._2._2.isDefined)
  .map(f => ((f._1._1, f._1._2, f._1._3, f._2._2.get), f._2._1))
  
 val joinNew = join.leftOuterJoin(dataset2)

 val join1 = joinNew
.filter(f => !f._2._2.isEmpty)
.map(f => (f._1, f._2._1.mkString(","), f._2._2.get)) 
join1.saveAsTextFile(rootpath + "join1")
      saveOneFile(rootpath + "join1")

       val join2 = joinNew
.filter(f => f._2._2.isEmpty)
.map(f => (f._1, f._2._1.mkString(","), f._2._2.get)) 
join2.saveAsTextFile(rootpath + "join2")
      saveOneFile(rootpath + "join2")

*/


    /*
  val tt = List(1, 2, 3, 4)
  val ss = List(1, 0)
  val yy = List(2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015)

  for (s <- ss) {
    for (t <- tt) {
      for (y <- yy) {
        
        val nextId = { var i = 0; () => { i += 1; i } }
        val dataset2 = dataset.filter { x => (x._1 == t && x._2 == s && x._3 == y) }
          .sortBy(x => (x._1, x._2, x._3, x._4), true)
          
        var list = (1 to dataset2.count().toInt).toList  
          
        val dataset3 = dataset2.map { x =>  
          val ind = list.head
          list = list.tail
          (x._1, x._2, x._3, x._4, ind) }
          println(s + " - " + t + " - " + y)

          dataset3.foreach(println)/*.saveAsTextFile(rootpath + "d-" + t + "-" + y + "-" + s)
      saveOneFile(rootpath + "d-" + t + "-" + y + "-" + s)*/
      }
    }
  }
*/
}