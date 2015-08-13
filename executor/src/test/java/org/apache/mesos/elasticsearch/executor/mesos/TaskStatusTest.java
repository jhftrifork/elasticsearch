package org.apache.mesos.elasticsearch.executor.mesos;

import org.apache.mesos.Protos;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Tests
 */
public class TaskStatusTest {
    private TaskStatus status = new TaskStatus();
    @Test(expected = NullPointerException.class)
    public void shouldExceptionIfPassedNull() {
        status.setTaskID(null);
    }

    @Test
    public void shouldReturnValidProtos() {
        status.setTaskID(Protos.TaskID.newBuilder().setValue("").build());
        status.setTaskState(Protos.TaskState.TASK_STARTING, null);
        assertNotNull(status.setIsStarting());
        status.setTaskState(Protos.TaskState.TASK_RUNNING, null);
        assertNotNull(status.setIsRunning());
        status.setTaskState(Protos.TaskState.TASK_FINISHED, null);
        assertNotNull(status.setIsFinished());
        status.setTaskState(Protos.TaskState.TASK_FAILED, null);
        assertNotNull(status.setIsFailed());
        status.setTaskState(Protos.TaskState.TASK_ERROR, null);
        assertNotNull(status.setIsError());
    }
}