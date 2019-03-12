package org.eisti.mlservice.handler

import org.eisti.mlservice.recommendation.MovieRecommendation
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation.{CrossOrigin, RequestMapping, RestController}
import org.springframework.web.bind.annotation.RequestMethod.GET

@RestController
@RequestMapping(path = Array("/api/movie"))
class MovieRecommendationHandler(@Autowired movieRecommendation: MovieRecommendation) {
  
  @CrossOrigin(origins = Array("*"))
  @RequestMapping(path = Array("imdb-score") , method = Array(GET))
    def predictMoviesIMDBScores(): Map[String, Any] = movieRecommendation.predictMoviesIMDBScores()
}
