package org.apache.mesos.elasticsearch.configuration.webserver.controller;

import junit.framework.Assert;
import org.junit.Test;
import org.springframework.http.HttpStatus;

import static org.junit.Assert.*;

/**
 * Tests fileserver
 */
public class FileServerTest {
    @Test
    public void testHealthCheck() {
        FileServer fileServer = new FileServer();
        assertTrue(fileServer.healthcheck().equals(HttpStatus.OK));
    }
}