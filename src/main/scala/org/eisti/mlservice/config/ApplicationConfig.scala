package org.eisti.mlservice.config

import org.apache.spark.sql.SparkSession
import org.springframework.beans.factory.annotation.{Autowired, Value}
import org.springframework.context.annotation.{Bean, Configuration, PropertySource}
import org.apache.spark.{SparkConf, SparkContext}
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer
import org.springframework.core.env.Environment

@Configuration
@PropertySource(Array("classpath:application.properties"))
class ApplicationConfig() {

  @Autowired
  var environment: Environment = null

  @Value("#{environment['master.uri'] ?: 'local[*]'}")
  val masterUri:String = null
  @Value("#{environment['app.name'] ?: 'eisti.ml.service'}")
  val appName:String = null
  @Value("#{environment['spark.sql.warehouse.dir='] ?: '/Users/aytacozkan/data-bi'}")
  val warehouse : String = null

  
  @Bean
  val props = ApplicationConfig.props

  @Bean
  def sparkConf():SparkConf = {
    val sparkConf = new SparkConf()
      .setAppName(appName)
      .setMaster(masterUri)
      .set("spark.driver.host", "127.0.0.1")
      .set("spark.sql.shuffle.partitions", "6")
      .set("spark.executor.memory", "2g")
      .set("spark.sql.warehouse.dir", warehouse)

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


object ApplicationConfig {
  @Bean
  def props = {
     new PropertySourcesPlaceholderConfigurer()
  }
}
