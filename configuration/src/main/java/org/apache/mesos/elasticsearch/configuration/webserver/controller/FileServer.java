package org.apache.mesos.elasticsearch.configuration.webserver.controller;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * Class to serve files
 */
@RestController
@RequestMapping("/v1/configuration")
public class FileServer {
    @RequestMapping(value = "/health", method = RequestMethod.GET)
    public HttpStatus healthcheck() {
        return HttpStatus.OK;
    }
}
