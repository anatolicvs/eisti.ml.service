package org.eisti.mlservice.recommendation


import org.apache.spark.ml.Pipeline
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector}
import org.springframework.beans.factory.annotation.{Autowired, Value}
import org.springframework.core.env.Environment
import org.springframework.stereotype.Service


case class Movie(color: String,
                 director_name:String,
                 num_critic_for_reviews:Int,
                 duration:Int,
                 director_facebook_likes:Int,
                 actor_3_facebook_likes:Int,
                 actor_2_name:String,
                 actor_1_facebook_likes:Int,
                 gross:Int,
                 genres: String,
                 actor_1_name: String,
                 movie_title:String,
                 num_voted_users:Int,
                 cast_total_facebook_likes: Int,
                 actor_3_name:String,
                 facenumber_in_poster:Int,
                 plot_keywords:String,
                 movie_imdb_link:String,
                 num_user_for_reviews:Int,
                 language:String,
                 country:String,
                 content_rating: String,
                 budget:Long,
                 title_year: Int,
                 actor_2_facebook_likes:Int,
                 imdb_score:Double,
                 aspect_ratio:Double,
                 movie_facebook_likes:Int)

case class PMovieScore(prevImdbScore:Int, predictedImdbScore:Int)

@Service
class MovieRecommendation(@Autowired
                           val spark:SparkSession) {

  @Autowired
  var environment: Environment = null

  @Value("#{environment['movies.data.file'] ?: '/Users/aytacozkan/data-bi/data/movie_metadata.csv'}")
  val dataUri:String = null

    
    def computeRmse(model:MatrixFactorizationModel,data: RDD[Rating],implicitPrefs:Boolean):Double = {

      val predictions: RDD[Rating] = model.predict(data.map(x => (x.user,x.product)))
      val predictionsAndRatings = predictions.map { x => ((x.user,x.product),x.rating) }.join(data.map(x => ((x.user, x.product),x.rating))).values
      if(implicitPrefs) {
        println("(Prediction, Rating)")
        println(predictionsAndRatings.take(5).mkString("\n"))
      }
      math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).mean())
    }

    def predictMoviesIMDBScores(): Map[String, Any]  = {
      
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
        .option("ignoreCorruptFiles",true)
        .option("nullValue", false)
        .format("csv")
        .load(dataUri)
        .na.fill(0)


      val movieDF = df.as[Movie]

      movieDF.createOrReplaceTempView("movies")

      val moviesqlresult = spark.sql("SELECT movie_title,imdb_score FROM movies desc limit 20")

      moviesqlresult.show(true)
      
      /*
      val moviesDF = df.select(df.col("movie_title"),
                                df.col("genres"),
                                df.col("num_user_for_reviews"),
                                df.col("cast_total_facebook_likes"))

      df.schema.foreach(field => println(field.dataType + ": " + field.name))
      */
      
      val featureCols = Array(
        "color",
        "director_facebook_likes",
        "actor_3_facebook_likes",
        "actor_2_name",
        "actor_1_facebook_likes",
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
      val indexedDF = pipeline.fit(movieDF).transform(movieDF)
      indexedDF.show()

      val indexer = new StringIndexer()
        .setInputCol("movie_title_indexed")
        .setOutputCol("label")

      val labelIndexedDF = indexer.fit(indexedDF).transform(indexedDF)

      // predict gross
      val newIndexedDF = labelIndexedDF.select(
        "label",
        "imdb_score",
        "gross",
        "title_year"
       )

      val newFeatureCols = newIndexedDF.columns

      val assembler = new VectorAssembler()
        .setInputCols(newFeatureCols)
        .setOutputCol("features")

      val assembledDF = assembler.transform(newIndexedDF)
      assembledDF.show()


      val finalDF = assembledDF.select("label", "features")

      //val resultDF = finalDF.map(row => PMovieScore(row.getInt(1), row.getInt(2)))

      finalDF.show(true)

      finalDF.schema.foreach(field => println(field.dataType + ": " + field.name))
      

      return Map("data" -> finalDF.select("features").rdd.map(_.getAs[DenseVector](0).values).take(100)
      )
    }
}
