package org.eisti.mlservice.util

import org.apache.spark.sql.SparkSession

object SparkSessionCreate {

  def createSparkSession(appName: String) :SparkSession = {
  
    val spark = SparkSession
      .builder
      .master("spark://0.0.0.0:7077")
      .config("spark.sql.warehouse.dir", "/Users/aytacozkan/local.works")
      .appName(appName)
      .getOrCreate()

    return spark
  }
}
