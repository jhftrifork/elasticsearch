package org.apache.mesos.elasticsearch.systemtest;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static com.jayway.awaitility.Awaitility.await;

/**
 * Response which waits until tasks endpoint is ready
 */
public class TasksResponse {

    public static final Logger LOGGER = Logger.getLogger(DiscoverySystemTest.class);

    private HttpResponse<JsonNode> response;
    private String schedulerIpAddress;
    private String schedulerManagementPort;
    private int tasksCount;

    public TasksResponse(String schedulerIpAddress, String schedulerManagementPort, int tasksCount) {
        this.schedulerIpAddress = schedulerIpAddress;
        this.schedulerManagementPort = schedulerManagementPort;
        this.tasksCount = tasksCount;
        LOGGER.info("Waiting for tasks at endpoint: \"" + getEndpoint() + "\"");
        await().atMost(60, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).until(new TasksCall());
    }

    class TasksCall implements Callable<Boolean> {

        @Override
        public Boolean call() throws Exception {
            try {
                String tasksEndPoint = getEndpoint();
                LOGGER.info("Fetching tasks on " + tasksEndPoint);
                response = Unirest.get(tasksEndPoint).asJson();
                int numTasks = response.getBody().getArray().length();
                if (numTasks == 3) {
                    return true;
                } else {
                    LOGGER.info("Waiting for " + tasksCount + " tasks, but only " + numTasks + " have started");
                    return false;
                }
            } catch (UnirestException e) {
                LOGGER.info("Exception when attempting to fetch tasks: " + e.getMessage());
                return false;
            }
        }
    }

    private String getEndpoint() {
        return "http://" + schedulerIpAddress + ":" + schedulerManagementPort + "/v1/tasks";
    }

    public HttpResponse<JsonNode> getJson() {
        return response;
    }

    public List<JSONObject> getTasks() {
        List<JSONObject> tasks = new ArrayList<>();
        for (int i = 0; i < response.getBody().getArray().length(); i++) {
            tasks.add(response.getBody().getArray().getJSONObject(i));
        }
        return tasks;
    }
}
