package org.eisti.mlservice.recommendation

import java.time.LocalDateTime

import org.apache.spark.{SparkConf, SparkContext}
import org.eisti.mlservice.util.SparkSessionCreate
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.springframework.beans.factory.annotation.Autowired
import scala.math.random

import scala.io.Source

object MovieRecommendation {
  
    def computeRmse(model:MatrixFactorizationModel,data: RDD[Rating],implicitPrefs:Boolean):Double = {

      val predictions: RDD[Rating] = model.predict(data.map(x => (x.user,x.product)))
      val predictionsAndRatings = predictions.map { x => ((x.user,x.product),x.rating) }.join(data.map(x => ((x.user, x.product),x.rating))).values
      if(implicitPrefs) {
        println("(Prediction, Rating)")
        println(predictionsAndRatings.take(5).mkString("\n"))
      }
      math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).mean())
    }

    def recommendMovies(): Map[String, Any]  = {

      //val spark : SparkSession = SparkSessionCreate.createSparkSession("MovieRecommendation")
      /*
      val conf = new SparkConf()
        .setMaster("spark://0.0.0.0:7077")
        .setAppName("MovieRecommendation")
      
      val sc = SparkContext.getOrCreate(conf)
      */

      // Create a SparkSession. No need to create SparkContext
      // You automatically get it as part of the SparkSession
      // val warehouseLocation = "file:${system:user.dir}/spark-warehouse"
      /*
      val spark = SparkSession
        .builder()
        .master("spark://0.0.0.0:7077")
        .appName("MovieRecommendation")
        .config("spark.sql.warehouse.dir", warehouseLocation)
        .enableHiveSupport()
        .getOrCreate()
      */
      /*
      val conf = new SparkConf()
        .setAppName("MovieRecommendation")
        .setMaster("spark://0.0.0.0:7077")
        .set("spark.driver.host", "127.0.0.1");

      val sc = SparkContext.getOrCreate(conf)
      */
      //set new runtime options
      //spark.conf.set("spark.sql.shuffle.partitions", 6)
      //spark.conf.set("spark.executor.memory", "2g")
    


      //val ratigsFile = "data/IMDB_Movies_Ratings.csv"
      //val df1 = spark.read.format("com.databricks.spark.csv").option("header", true).load(ratigsFile)



      return Map("Message" -> "Spring Boot Scala", "today" -> LocalDateTime.now().toString)
    }
}
