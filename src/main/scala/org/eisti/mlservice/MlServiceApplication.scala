package org.eisti.mlservice

import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication

@SpringBootApplication
class MlServiceApplication

object MlServiceApplication{
  def main(args: Array[String]): Unit = SpringApplication.run(classOf[MlServiceApplication], args: _*)
}
