package org.eisti.mlservice.handler

import org.eisti.mlservice.recommendation.MovieRecommendation
import org.springframework.web.bind.annotation.{RequestMapping, RestController}
import org.springframework.web.bind.annotation.RequestMethod.GET

@RestController
@RequestMapping(path = Array("/recommend-movies"))
class MovieRecommendationHandler {
    @RequestMapping(method = Array(GET))
    def recommendMovies(): Map[String, Any] = {
         MovieRecommendation.recommendMovies()
    }
}
