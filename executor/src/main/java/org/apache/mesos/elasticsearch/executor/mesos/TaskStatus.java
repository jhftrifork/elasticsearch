package org.apache.mesos.elasticsearch.executor.mesos;

import org.apache.log4j.Logger;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.elasticsearch.common.lang3.Validate;

/**
 * Models the state of the single task launched on this executor.
 */
public class TaskStatus {
    private static final Logger LOGGER = Logger.getLogger(TaskStatus.class.getCanonicalName());

    // Object invariant: (taskId == null) == (taskState == null)
    private Protos.TaskID taskId = null;
    private Protos.TaskState taskState = null;

    // Precondition: this.taskId != null
    public void setTaskState(Protos.TaskID taskId, Protos.TaskState newTaskState, ExecutorDriver driver) {
        LOGGER.info("Setting task \"" + taskId.getValue() + "\" to state \"" + newTaskState.name() + "\"");
        Validate.isTrue(this.taskId == taskId, "The Elasticsearch executor cannot launch multiple tasks");
        taskState = newTaskState;
        driver.sendStatusUpdate(getTaskStatus());
    }

    // Precondition: this.taskId == null
    public void setTaskId(Protos.TaskID taskId) {
        Validate.isTrue(this.taskId == null, "The Elasticsearch executor cannot launch multiple tasks");
        this.taskId = taskId;
        taskState = Protos.TaskState.TASK_STAGING;
    }

    public Protos.TaskStatus getTaskStatus() {
        return Protos.TaskStatus.newBuilder()
                .setTaskId(taskId)
                .setState(taskState).build();
    }
}