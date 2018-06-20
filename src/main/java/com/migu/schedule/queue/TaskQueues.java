package com.migu.schedule.queue;

import com.migu.schedule.constants.TaskStatusEnums;
import com.migu.schedule.info.NodeInfo;
import com.migu.schedule.info.NodeTaskInfo;
import com.migu.schedule.info.TaskInfo;
import com.migu.schedule.info.TaskInfoInQueue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 任务队列
 * Created by spirit on 18/6/20.
 */
public class TaskQueues {

    public static final int MAX_HAND_UP_TASK_SIZE = 100;

    public static final int MAX_RUNING_TASK_SIZE = 100;

    /**
     * 任务总消耗资源
     */
    public static int sumConsumption = 0;

    /**
     * 任务消耗资源最小的
     */
    public static int minConsumption = Integer.MAX_VALUE;

    /**
     * 挂起任务
     */
    public static Map<Integer,TaskInfoInQueue> hangUpTaskMap = new HashMap<Integer, TaskInfoInQueue>(MAX_HAND_UP_TASK_SIZE);

    /**
     * 已加入的任务,按任务大小排好序
     */
    public static Set<Integer> exitTaskIdSet = new HashSet<Integer>(MAX_RUNING_TASK_SIZE + MAX_RUNING_TASK_SIZE);

    /**
     * 已加入的任务
     */
    public static  Map<Integer,TaskInfoInQueue> exitTaskMap = new HashMap<Integer, TaskInfoInQueue>(MAX_RUNING_TASK_SIZE + MAX_RUNING_TASK_SIZE);

    /**
     * 已注册的节点,按节点大小排好序
     */
    public static  Set<Integer> registeredNodeIdSet = new HashSet<Integer>(MAX_RUNING_TASK_SIZE);

    /**
     * 运行中节点任务
     */
    public static Map<Integer,NodeTaskInfo> runningNodeTaskInfoMap = new HashMap<Integer, NodeTaskInfo>(MAX_RUNING_TASK_SIZE);

    /**
     * 初始化各队列信息
     */
    public static void init(){
        TaskQueues.hangUpTaskMap = new HashMap<Integer, TaskInfoInQueue>(MAX_HAND_UP_TASK_SIZE);
        TaskQueues.exitTaskIdSet = new HashSet<Integer>(MAX_RUNING_TASK_SIZE + MAX_RUNING_TASK_SIZE);
        TaskQueues.exitTaskMap = new HashMap<Integer, TaskInfoInQueue>(MAX_RUNING_TASK_SIZE + MAX_RUNING_TASK_SIZE);
        TaskQueues.registeredNodeIdSet = new HashSet<Integer>(MAX_HAND_UP_TASK_SIZE);
        TaskQueues.runningNodeTaskInfoMap = new HashMap<Integer, NodeTaskInfo>(MAX_RUNING_TASK_SIZE);
    }

    /**
     * 增加一个挂起任务
     * @param taskId
     */
    public static void addHangUpTask(int taskId, int consumption){
        TaskInfoInQueue taskInfoInQueue = new TaskInfoInQueue();
        TaskInfo taskInfo = new TaskInfo();
        taskInfo.setTaskId(taskId);
        taskInfoInQueue.setTaskInfo(taskInfo);
        taskInfoInQueue.setConsumption(consumption);
        taskInfoInQueue.setTaskStatus(TaskStatusEnums.HAND_UP.getCode());
        TaskQueues.hangUpTaskMap.put(taskId, taskInfoInQueue);
        TaskQueues.exitTaskMap.put(taskId, taskInfoInQueue);
        TaskQueues.exitTaskIdSet.add(taskId);
        TaskQueues.sumConsumption = TaskQueues.sumConsumption + taskInfoInQueue.getConsumption();
        if(taskInfoInQueue.getConsumption()<TaskQueues.minConsumption){
            TaskQueues.minConsumption = taskInfoInQueue.getConsumption();
        }
    }

    /**
     * 移除一个任务
     * @param taskId
     */
    public static void removeTask(int taskId) {
        TaskInfoInQueue taskInfoInQueue = TaskQueues.exitTaskMap.get(taskId);
        if(taskInfoInQueue==null){
            return;
        }
        // 获取当前任务的id
        Integer nodeId = null;
        if (taskInfoInQueue != null && taskInfoInQueue.getTaskInfo() != null) {
            nodeId = taskInfoInQueue.getTaskInfo().getNodeId();
        }

        // 挂起移除
        if (taskInfoInQueue.getTaskStatus() == TaskStatusEnums.HAND_UP.getCode()) {
            TaskQueues.hangUpTaskMap.remove(taskId);
        }
        // 运行中将运行中任务移除
        if (taskInfoInQueue.getTaskStatus() == TaskStatusEnums.RUNNING.getCode()) {
            NodeTaskInfo nodeTaskInfo = TaskQueues.runningNodeTaskInfoMap.get(nodeId);
            if (nodeTaskInfo == null || nodeTaskInfo.getRunningTaskInfoList() == null
                || nodeTaskInfo.getRunningTaskInfoList().size() == 0) {
                return;
            }
            List<TaskInfoInQueue> runningTaskInfoList = nodeTaskInfo.getRunningTaskInfoList();
            for (int index = runningTaskInfoList.size() - 1; index >= 0; index--) {
                TaskInfoInQueue runningTaskInfoInQueue = runningTaskInfoList.get(index);
                if (runningTaskInfoInQueue != null && runningTaskInfoInQueue.getTaskInfo() != null
                    && Objects.equals(runningTaskInfoInQueue.getTaskInfo().getTaskId(), taskId)) {
                    runningTaskInfoList.remove(Integer.valueOf(taskId));
                }
            }
        }

        // 已存在的移除
        TaskQueues.exitTaskMap.remove(taskId);
        // 最后把id去除
        TaskQueues.exitTaskIdSet.remove(Integer.valueOf(taskId));
    }

    /**
     * 增加一个注册节点
     * @param nodeId
     */
    public static void addRegisteredNode(int nodeId){
        TaskQueues.registeredNodeIdSet.add(nodeId);
        NodeTaskInfo nodeTaskInfo = new NodeTaskInfo();
        NodeInfo nodeInfo = new NodeInfo();
        nodeInfo.setNodeId(nodeId);
        nodeTaskInfo.setNodeInfo(nodeInfo);
        TaskQueues.runningNodeTaskInfoMap.put(nodeId, nodeTaskInfo);
    }

    /**
     * 移除一个注册节点
     * @param nodeId
     */
    public static void removeRegisteredNode(int nodeId){

        TaskQueues.registeredNodeIdSet.remove(nodeId);
        NodeTaskInfo nodeTaskInfo = TaskQueues.runningNodeTaskInfoMap.get(nodeId);
        List<TaskInfoInQueue> runningTaskInfoList = nodeTaskInfo.getRunningTaskInfoList();
        //当前任务加入挂起
        if(runningTaskInfoList!=null&&runningTaskInfoList.size()>0) {
            for(TaskInfoInQueue taskInfoInQueue: runningTaskInfoList) {
                TaskQueues.hangUpTaskMap.put(taskInfoInQueue.getTaskInfo().getTaskId(), taskInfoInQueue);
            }
        }
        //移除节点
        TaskQueues.runningNodeTaskInfoMap.remove(nodeId);
    }

    public static boolean schedule(int threshold){

        //有挂起任务,则将挂起任务加入运行任务调度
        if(TaskQueues.hangUpTaskMap!=null&&TaskQueues.hangUpTaskMap.size()!=0){
            List<TaskInfoInQueue> taskList = new ArrayList<TaskInfoInQueue>(TaskQueues.hangUpTaskMap.values());
            Collections.sort(taskList, new Comparator<TaskInfoInQueue>() {
                //按消耗值降序排列
                public int compare(TaskInfoInQueue o1, TaskInfoInQueue o2) {
                    return o2.getConsumption()-o1.getConsumption();
                }
            });
            //先加入消耗值打的任务
            for(TaskInfoInQueue task: taskList){
                //放任务
                putOneTaskToRunning(task);
                TaskQueues.hangUpTaskMap.remove(task.getTaskInfo().getTaskId());
            }
            //调度成功
            return true;
        }
        Integer[] registeredNodeIdArr = (Integer[])TaskQueues.registeredNodeIdSet.toArray();
        for(int index=0;index<registeredNodeIdArr.length;index++){
            NodeTaskInfo nodeTaskInfo = TaskQueues.runningNodeTaskInfoMap.get(registeredNodeIdArr[index]);
            if(nodeTaskInfo==null){
                continue;
            }
            for(int lastIndex = index-1;lastIndex>0;lastIndex--){
                NodeTaskInfo lastNodeTaskInfo = TaskQueues.runningNodeTaskInfoMap.get(registeredNodeIdArr[lastIndex]);
                //总消耗率的差值小于等于调度阈值,则进行任务的迁移
                if(lastNodeTaskInfo!=null&&!diffLargerThanThreshold(nodeTaskInfo.getSumConsumption(), lastNodeTaskInfo.getSumConsumption(), threshold)){
                    TaskInfoInQueue taskInfoInQueue = nodeTaskInfo.getRunningTaskInfoList().get(0);
                    //任务调度到较前面的任务中
                    lastNodeTaskInfo.addTask(taskInfoInQueue);
                    nodeTaskInfo.getRunningTaskInfoList().remove(0);

                    //调度成功
                    return true;
                }
            }
        }

        //无调度
        return false;
    }

    /**
     * 资源消耗值是否大于阈值
     * @param consumption1 资源消耗1
     * @param consumption2 资源消耗2
     * @param threshold 阈值
     * @return
     */
    private static boolean diffLargerThanThreshold(int consumption1, int consumption2, int threshold) {
        return consumption1 - consumption2 > threshold || consumption2 - consumption1 > threshold;
    }

    private static boolean putOneTaskToRunning(TaskInfoInQueue taskInfoInQueue) {
        // 异常数据不处理
        if (taskInfoInQueue == null) {
            return false;
        }
        int avgConsumption = TaskQueues.sumConsumption / TaskQueues.registeredNodeIdSet.size();
        NodeTaskInfo minNodeTaskInfo = null;
        int minConsumptionInNode = Integer.MAX_VALUE;
        for (Integer nodeId : TaskQueues.registeredNodeIdSet) {
            NodeTaskInfo nodeTaskInfo = TaskQueues.runningNodeTaskInfoMap.get(nodeId);
            if (nodeTaskInfo == null) {
                continue;
            }
            if(minConsumptionInNode> nodeTaskInfo.getSumConsumption()){
                minConsumptionInNode = nodeTaskInfo.getSumConsumption();
                minNodeTaskInfo = nodeTaskInfo;
            }
            // 优先匹配到在均值消耗以下的节点,将任务排进去
            if (nodeTaskInfo.getSumConsumption() + taskInfoInQueue.getConsumption() <= avgConsumption) {
                nodeTaskInfo.addTask(taskInfoInQueue);
                return true;
            }
        }
        //都没匹配到,加到当前总消耗最小的
        minNodeTaskInfo.addTask(taskInfoInQueue);
        return false;
    }

    public static Map<Integer, TaskInfoInQueue> getHangUpTaskMap() {
        return hangUpTaskMap;
    }

    public static Set<Integer> getExitTaskIdSet() {
        return exitTaskIdSet;
    }

    public static Map<Integer, TaskInfoInQueue> getExitTaskMap() {
        return exitTaskMap;
    }

    public static Set<Integer> getRegisteredNodeIdSet() {
        return registeredNodeIdSet;
    }

    public static Map<Integer, NodeTaskInfo> getRunningNodeTaskInfoMap() {
        return runningNodeTaskInfoMap;
    }
}
