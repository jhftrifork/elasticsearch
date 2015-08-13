package org.apache.mesos.elasticsearch.executor.mesos;

import org.apache.log4j.Logger;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;

/**
 * Models the state of tasks launched on this executor.
 * TODO fix current limitation: it can only model a single task!
 */
public class TaskStatus {
    private static final Logger LOGGER = Logger.getLogger(TaskStatus.class.getCanonicalName());

    // Object invariant: (taskID == null) == (taskState == null)
    private Protos.TaskID taskID = Protos.TaskID.newBuilder().setValue("").build();
    private Protos.TaskState taskState = Protos.TaskState.TASK_STAGING;

    public void setTaskState(Protos.TaskState newTaskState, ExecutorDriver driver) {
        LOGGER.info("Setting task \"" + taskID.getValue() + "\" to state \"" + newTaskState.name() + "\"");
        taskState = newTaskState;
        driver.sendStatusUpdate(getTaskStatus());
    }

    public void setTaskID(Protos.TaskID newTaskID) {
        if (newTaskID == null) {
            throw new NullPointerException("TaskID cannot be null");
        }
        this.taskID = newTaskID;
    }

    public Protos.TaskStatus getTaskStatus() {
        return Protos.TaskStatus.newBuilder()
                .setTaskId(taskID)
                .setState(taskState).build();
    }
}