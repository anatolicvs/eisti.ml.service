package org.eisti.mlservice.recommendation

import java.time.LocalDateTime

import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier, LogisticRegression}
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.evaluation.{MulticlassClassificationEvaluator, RegressionEvaluator}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

@Service
class MovieRecommendation( @Autowired
                           val spark: SparkSession) {
  
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
      
      val conf = new SparkConf()
        .setAppName("MovieRecommendation")
        .setMaster("local[*]")
        .set("spark.driver.host", "127.0.0.1")
        .set("spark.sql.shuffle.partitions", "6")
        .set("spark.executor.memory", "2g")
        .set("spark.sql.warehouse.dir", "/Users/aytacozkan/data-bi")

      val spark1 = SparkSession
        .builder()
        .config(conf)
        .getOrCreate()

      import spark.implicits._

      val extractFirstGenre = udf((col: String) => col.split('|')(0))
      val extractSecondGenre = udf((col: String) => {
        val items = col.split('|').tail
        if (items.length == 0)
          "None"
        else
          items(0)
      })

      val df = spark
        .read
        .option("header", true)
        .option("inferSchema", true)
        .option("ignoreLeadingWhiteSpace", true)
        .option("ignoreTrainingWhiteSpace", true)
        //.option("ignoreCorruptFiles",true)
        .option("nullValue", "None")
        .format("csv")
        .load("/Users/aytacozkan/data-bi/data/movie_metadata.csv")
        .withColumn("first_genre", extractFirstGenre($"genres"))
        .withColumn("second_genre", extractSecondGenre($"genres"))
        .na.fill(0)
      

      val moviesDF = df.select(df.col("movie_title"),
                                df.col("genres"),
                                df.col("num_user_for_reviews"),
                                df.col("cast_total_facebook_likes"))

      moviesDF.createOrReplaceTempView("movies")


      df.schema.foreach(field => println(field.dataType + ": " + field.name))
      
      val featureCols = Array(
        "color",
        "director_name",
        "genres",
        "actor_1_name",
        "movie_title",
         "plot_keywords",
         "title_year")

      //val featureCol = trainingDF.columns
      var indexers: Array[StringIndexer] = Array()

      featureCols.map { colName =>
        val index = new StringIndexer()
          .setInputCol(colName)
          .setOutputCol(colName + "_indexed")
          .setHandleInvalid("skip")
        indexers = indexers :+ index
      }

      val pipeline = new Pipeline().setStages(indexers)
      val indexedDF = pipeline.fit(df).transform(df)
      indexedDF.show()

      val indexer = new StringIndexer()
        .setInputCol("movie_title_indexed")
        .setOutputCol("label")

      val labelIndexedDF = indexer.fit(indexedDF).transform(indexedDF)

      // predict gross
      val newIndexedDF = labelIndexedDF.select(
        "label",
        "gross", "imdb_score"
       )

      val newFeatureCols = newIndexedDF.columns

      val assembler = new VectorAssembler()
        .setInputCols(newFeatureCols)
        .setOutputCol("features")

      val assembledDF = assembler.transform(newIndexedDF)
      assembledDF.show()

      val finalDF = assembledDF.select("label", "features")
      finalDF.show()
      
      //spark.close()

      return Map("Message" -> "Spring Boot Scala", "today" -> LocalDateTime.now().toString)
    }
}
