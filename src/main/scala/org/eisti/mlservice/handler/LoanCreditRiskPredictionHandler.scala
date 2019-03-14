package org.eisti.mlservice.handler

import org.eisti.mlservice.prediction.LoanCreditRiskPrediction
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation.{CrossOrigin, RequestMapping, RestController}
import org.springframework.web.bind.annotation.RequestMethod.GET

@RestController
@RequestMapping(path = Array("/api/credit"))
class LoanCreditRiskPredictionHandler(@Autowired loanCreditRiskPrediction:LoanCreditRiskPrediction) {

  @CrossOrigin(origins = Array("*"))
  @RequestMapping(path = Array("risk"), method = Array(GET))
  def predictLoadCreditRisk(): Map[String,Any] =loanCreditRiskPrediction.predictLoadCreditRisk()

}
