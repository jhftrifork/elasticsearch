package org.apache.mesos.elasticsearch.executor.mesos;

import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.*;

/**
 * Tests
 */
public class TaskStatusTest {
    private TaskStatus status = new TaskStatus();

    @Test
    public void whenStartingTask() {
        Protos.TaskID taskId = Protos.TaskID.newBuilder().setValue("some_task_id").build();
        Protos.TaskState taskState = Protos.TaskState.TASK_STARTING;

        ExecutorDriver mockDriver = Mockito.mock(ExecutorDriver.class);
        Mockito.when(mockDriver.sendStatusUpdate(Mockito.any())).thenReturn(null);

        status.setTaskState(taskId, taskState, mockDriver);

        Mockito.verify(mockDriver, Mockito.times(1)).sendStatusUpdate(
                Protos.TaskStatus.newBuilder()
                        .setTaskId(taskId)
                        .setState(taskState).build());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionIfTaskIdIsSetTwice() {
        ExecutorDriver mockDriver = Mockito.mock(ExecutorDriver.class);
        Mockito.when(mockDriver.sendStatusUpdate(Mockito.any())).thenReturn(null);

        status.setTaskState(Protos.TaskID.newBuilder().setValue("some_task_id").build(), Protos.TaskState.TASK_STARTING, mockDriver);
        status.setTaskState(Protos.TaskID.newBuilder().setValue("some_other_task_id").build(), Protos.TaskState.TASK_STARTING, mockDriver);
    }

    @Test
    public void shouldReturnTaskStatusForCurrentState() {
        ExecutorDriver mockDriver = Mockito.mock(ExecutorDriver.class);
        Mockito.when(mockDriver.sendStatusUpdate(Mockito.any())).thenReturn(null);

        status.setTaskState(Protos.TaskID.newBuilder().setValue("some_task_id").build(), Protos.TaskState.TASK_FAILED, mockDriver);

        assertEquals(
                status.getTaskStatus(),
                Protos.TaskStatus.newBuilder()
                    .setTaskId(Protos.TaskID.newBuilder().setValue("some_task_id").build())
                    .setState(Protos.TaskState.TASK_FAILED).build()
        );
    }

//    @Test
//    public void shouldReturnValidProtos() {
//        status.setTaskId(Protos.TaskID.newBuilder().setValue("some_task_id").build());
//        status.
//        status.setTaskState(Protos.TaskState.TASK_STARTING, null);
//        assertNotNull(status.setIsStarting());
//        status.setTaskState(Protos.TaskState.TASK_RUNNING, null);
//        assertNotNull(status.setIsRunning());
//        status.setTaskState(Protos.TaskState.TASK_FINISHED, null);
//        assertNotNull(status.setIsFinished());
//        status.setTaskState(Protos.TaskState.TASK_FAILED, null);
//        assertNotNull(status.setIsFailed());
//        status.setTaskState(Protos.TaskState.TASK_ERROR, null);
//        assertNotNull(status.setIsError());
//    }
}