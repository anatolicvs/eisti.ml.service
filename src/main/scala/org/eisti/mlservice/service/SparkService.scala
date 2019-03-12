package org.eisti.mlservice.service

trait SparkService {
  def mlService()
  def getVersion() : Long
  def getAvailableVersions() : Array[Long]
}
