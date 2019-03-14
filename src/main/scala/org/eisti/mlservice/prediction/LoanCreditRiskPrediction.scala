package org.eisti.mlservice.prediction

import java.io.Serializable

import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.springframework.beans.factory.annotation.{Autowired, Value}
import org.springframework.core.env.Environment
import org.springframework.stereotype.Service

case class Credit(
                   creditability: Double,
                   balance: Double, duration: Double, history: Double, purpose: Double, amount: Double,
                   savings: Double, employment: Double, instPercent: Double, sexMarried: Double, guarantors: Double,
                   residenceDuration: Double, assets: Double, age: Double, concCredit: Double, apartment: Double,
                   credits: Double, occupation: Double, dependents: Double, hasPhone: Double, foreign: Double
                 ) extends Serializable

@Service
class LoanCreditRiskPrediction(@Autowired val spark: SparkSession)  {
  
  @Autowired
  var environment: Environment = null

  @Value("#{environment['credit.risk.data.file'] ?: '/Users/aytacozkan/data-bi/data/germancredit.csv'}")
  val dataUri:String = null
  
  def parseCredit(line: Array[Double]): Credit = {
    Credit(
      line(0),
      line(1) - 1, line(2), line(3), line(4), line(5),
      line(6) - 1, line(7) - 1, line(8), line(9) - 1, line(10) - 1,
      line(11) - 1, line(12) - 1, line(13), line(14) - 1, line(15) - 1,
      line(16) - 1, line(17) - 1, line(18) - 1, line(19) - 1, line(20) - 1
    )
  }

  def parseRDD(rdd: RDD[String]): RDD[Array[Double]]  = {
    rdd.map(_.split(
      ","
    )).map(_.map(_.toDouble))
  }


  def predictLoadCreditRisk() : Map[String, Any] = {

    // import spark.implicits._
    // import spark._
    
    val credits = parseRDD(spark.sparkContext.textFile(dataUri)).collect().map(parseCredit)

    val creditDF = spark.createDataFrame(credits).cache()
    
    creditDF.createTempView("credit")

    // creditDF.printSchema

    // creditDF.show
    // computes statistics for balance**
    // creditDF.describe("balance").show
    // compute the avg balance by creditability (the label)**
    // creditDF.groupBy("creditability").avg("balance").show

    // Compute the average balance, amount, duration grouped by creditability**
    spark.sql("SELECT creditability, avg(balance) as avgbalance, avg(amount) as avgamt, avg(duration) as avgdur  FROM credit GROUP BY creditability ").show

    /*
    Extract Features

      To build a classifier model, you first extract the features that most contribute to the classification.
      In the german credit data set the data is labeled with two classes – 1 (creditable) and 0 (not creditable).

      The features for each item consists of the fields shown below:

      Label → creditable: 0 or 1
      Features → {"balance", "duration", "history", "purpose", "amount", "savings", "employment", "instPercent", "sexMarried", "guarantors", "residenceDuration", "assets", "age", "concCredit", "apartment", "credits", "occupation", "dependents", "hasPhone", "foreign"}
       
      In order for the features to be used by a machine learning algorithm, the features are transformed and put into Feature Vectors, which are vectors of numbers representing the value for each feature.

      Below a VectorAssembler is used to transform and return a new dataFrame with all of the feature columns in a vector column
    */
    
    //define the feature columns to put in the feature vector**
    val featureCols = Array("balance", "duration", "history", "purpose", "amount",
      "savings", "employment", "instPercent", "sexMarried",  "guarantors",
      "residenceDuration", "assets",  "age", "concCredit", "apartment",
      "credits",  "occupation", "dependents",  "hasPhone", "foreign" )
    
    // set the input and output column names**
    val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
    
    // return a dataFrame with all of the  feature columns in  a vector column**
    val df2 = assembler.transform(creditDF)
    // the transform method produced a new column: features.**
    df2.show

    // Next, we use a StringIndexer to return a DataFrame with the creditability column added as a label .
    //  Create a label column with the StringIndexer**
    val labelIndexer = new StringIndexer().setInputCol("creditability").setOutputCol("label")
    val df3 = labelIndexer.fit(df2).transform(df2)
    
    // the  transform method produced a new column: label.**
    df3.show

    // Below the data it is split into a training data set and a test data set, 70% of the data is used to train the model, 30% will be used for testing.

    //  split the dataframe into training and test data**
    val splitSeed = 5043
    val Array(trainingData, testData) = df3.randomSplit(Array(0.7, 0.3), splitSeed)

    /*
    Next, we train a RandomForest Classifier with the parameters:

      maxDepth: Maximum depth of a tree. Increasing the depth makes the model more powerful, but deep trees take longer to train.
      maxBins: Maximum number of bins used for discretizing continuous features and for choosing how to split on features at each node.
      impurity:Criterion used for information gain calculation

    auto:Automatically select the number of features to consider for splits at each tree node
    seed:Use a random seed number , allowing to repeat the results
    The model is trained by making associations between the input features and the labeled output associated with those features.
    */
    
    // create the classifier,  set parameters for training**
    val classifier = new RandomForestClassifier().setImpurity("gini").setMaxDepth(3).setNumTrees(20).setFeatureSubsetStrategy("auto").setSeed(5043)
    //  use the random forest classifier  to train (fit) the model**
    val model = classifier.fit(trainingData)

    // print out the random forest trees**
    model.toDebugString
    
    // Next we use the test data to get predictions.
    // run the  model on test features to get predictions**
    val predictions = model.transform(testData)
    //As you can see, the previous model transform produced a new columns: rawPrediction, probablity and prediction.**
    predictions.show
    /*
    Below we evaluate the predictions, we use a BinaryClassificationEvaluator which returns a precision metric (The Area Under an ROC Curve) by comparing the test label column with the test prediction column. In this case the evaluation returns 78% precision.
     */

    // create an Evaluator for binary classification, which expects two input columns: rawPrediction and label.**
    val evaluator = new BinaryClassificationEvaluator().setLabelCol("label")
    // Evaluates predictions and returns a scalar metric areaUnderROC(larger is better).**
    val accuracy = evaluator.evaluate(predictions)
    
    return Map("data" -> predictions.select("label","features").rdd.map(_.getAs[Double](0)).take(100))
  }

}
