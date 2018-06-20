package com.migu.schedule.info;

import com.migu.schedule.constants.TaskStatusEnums;

import java.util.ArrayList;
import java.util.List;

/**
 * 节点任务信息
 * Created by spirit on 18/6/20.
 */
public class NodeTaskInfo {

    /**
     * 当前节点
     */
    private NodeInfo nodeInfo;

    /**
     * 最大任务数
     */
    private int maxTaskSize = 100;

    /**
     * 总消耗
     */
    private int sumConsumption = 0;

    /**
     * 节点中任务
     */
    private List<TaskInfoInQueue> runningTaskInfoList;

    /**
     * 增加一个任务
     * @param taskInfoInQueue
     */
    public void addTask(TaskInfoInQueue taskInfoInQueue) {
        if (taskInfoInQueue == null || taskInfoInQueue.getTaskInfo() == null) {
            return;
        }
        taskInfoInQueue.getTaskInfo().setNodeId(this.getNodeInfo().getNodeId());
        taskInfoInQueue.setTaskStatus(TaskStatusEnums.RUNNING.getCode());
        if(this.runningTaskInfoList==null||this.runningTaskInfoList.size()==0){
            this.runningTaskInfoList = new ArrayList<TaskInfoInQueue>(this.maxTaskSize);
        }
        this.runningTaskInfoList.add(taskInfoInQueue);
        this.sumConsumption = this.sumConsumption + taskInfoInQueue.getConsumption();
    }
    
    public NodeInfo getNodeInfo() {
        return nodeInfo;
    }

    public void setNodeInfo(NodeInfo nodeInfo) {
        this.nodeInfo = nodeInfo;
    }

    public int getMaxTaskSize() {
        return maxTaskSize;
    }

    public void setMaxTaskSize(int maxTaskSize) {
        this.maxTaskSize = maxTaskSize;
    }

    public int getSumConsumption() {
        return sumConsumption;
    }

    public void setSumConsumption(int sumConsumption) {
        this.sumConsumption = sumConsumption;
    }

    public List<TaskInfoInQueue> getRunningTaskInfoList() {
        return runningTaskInfoList;
    }

    public void setRunningTaskInfoList(List<TaskInfoInQueue> runningTaskInfoList) {
        this.runningTaskInfoList = runningTaskInfoList;
    }
}
