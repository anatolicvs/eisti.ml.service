package org.eisti.mlservice.handler

import java.time.LocalDateTime

import org.springframework.web.bind.annotation.RequestMethod.GET
import org.springframework.web.bind.annotation.{RequestMapping, RestController}

@RestController
class TestHandler {
    @RequestMapping(path = Array("/"), method = Array(GET))
    def test(): Map[String, Any] = {
        Map("Message" -> "Spring Boot Scala", "today" -> LocalDateTime.now().toString)
    }
}
