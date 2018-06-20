package com.migu.schedule.info;

/**
 * 队列中的任务
 * Created by spirit on 18/6/20.
 */
public class TaskInfoInQueue{

    /**
     * 任务基础信息
     */
    private TaskInfo taskInfo;

    /**
     * 消耗资源,初始为0
     */
    private int consumption = 0;

    /**
     * 任务状态
     * @see com.migu.schedule.constants.TaskStatusEnums
     */
    private Integer taskStatus;

    @Override public String toString() {
        return "TaskInfoInQueue{" +
               "taskInfo=" + taskInfo +
               ", consumption=" + consumption +
               ", taskStatus=" + taskStatus +
               '}';
    }

    public TaskInfo getTaskInfo() {
        return taskInfo;
    }

    public void setTaskInfo(TaskInfo taskInfo) {
        this.taskInfo = taskInfo;
    }

    public int getConsumption() {
        return consumption;
    }

    public void setConsumption(int consumption) {
        this.consumption = consumption;
    }

    public Integer getTaskStatus() {
        return taskStatus;
    }

    public void setTaskStatus(Integer taskStatus) {
        this.taskStatus = taskStatus;
    }
}
