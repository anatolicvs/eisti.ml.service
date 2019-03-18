package org.eisti.mlservice.handler

import org.eisti.mlservice.prediction.BreastCancerPrediction
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation.{CrossOrigin, RequestMapping, RestController}

@RestController
@RequestMapping(path = Array("/api/cancer"))
class BreastCancerPredictionHandler(@Autowired val breastCancerPrediction: BreastCancerPrediction ) {

  @CrossOrigin(origins = Array("*"))
  @RequestMapping(path = Array("risk"))
  def predictBreastCancer() : Map[String, Any] = breastCancerPrediction.predictCancerRisk()
}
