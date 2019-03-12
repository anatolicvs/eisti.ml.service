package org.eisti.mlservice.config

import org.apache.spark.sql.SparkSession
import org.springframework.beans.factory.annotation.{Autowired, Value}
import org.springframework.context.annotation.{Bean, Configuration, PropertySource}
import org.apache.spark.{SparkConf, SparkContext}
import org.springframework.core.env.Environment

@Configuration
@PropertySource(Array("classpath:application.properties"))
class ApplicationConfig(@Value("${app.name:eisti.ml.service}")
                        val appName:String,
                        @Value("${master.uri:local}")
                        val masterUri:String
                       ) {

  @Autowired
  var environment: Environment = null

  @Bean
  val props = ApplicationConfig.props

  @Bean
  def sparkConf():SparkConf = {
    val sparkConf = new SparkConf()
      .setAppName(appName)
      .setMaster("local[*]")
      .set("spark.driver.host", "127.0.0.1")
      .set("spark.sql.shuffle.partitions", "6")
      .set("spark.executor.memory", "2g")
      .set("spark.sql.warehouse.dir", "/Users/aytacozkan/data-bi")

    return sparkConf
  }
  
  @Bean
  def sparksession(): SparkSession = {
    val sparkSession =   SparkSession
      .builder()
      .config(sparkConf())
      .getOrCreate()

    return sparkSession
  }

}
