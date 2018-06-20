package com.migu.schedule;

import com.migu.schedule.constants.ReturnCodeKeys;
import com.migu.schedule.constants.TaskStatusEnums;
import com.migu.schedule.info.TaskInfo;
import com.migu.schedule.info.TaskInfoInQueue;
import com.migu.schedule.queue.TaskQueues;

import java.util.List;
import java.util.Objects;

/*
*类名和方法不能修改
 */
public class Schedule {


    public int init() {
        //初始化任务队列
        TaskQueues.init();
        return ReturnCodeKeys.E001;
    }


    public int registerNode(int nodeId) {
        //节点编号小于0
        if(nodeId<=0){
            return ReturnCodeKeys.E004;
        }
        //节点存在
        if(TaskQueues.getRegisteredNodeIdSet().contains(nodeId)){

            return ReturnCodeKeys.E005;
        }
        //增加一个注册节点
        try {
            TaskQueues.addRegisteredNode(nodeId);
        }catch (Exception e){
            //异常
            return ReturnCodeKeys.E000;
        }
        return ReturnCodeKeys.E003;
    }

    public int unregisterNode(int nodeId) {
        //节点编号小于0
        if(nodeId<=0){
            return ReturnCodeKeys.E004;
        }
        //节点不存在
        if(!TaskQueues.getRegisteredNodeIdSet().contains(nodeId)){

            return ReturnCodeKeys.E007;
        }
        //移除一个注册节点的任务
        try {
            TaskQueues.removeRegisteredNode(nodeId);
        }catch (Exception e){
            //异常
            return ReturnCodeKeys.E000;
        }
        return ReturnCodeKeys.E006;
    }


    public int addTask(int taskId, int consumption) {
        //任务编号小于0
        if(taskId<=0){
            return ReturnCodeKeys.E009;
        }
        //任务存在
        if(TaskQueues.getExitTaskIdSet().contains(taskId)){

            return ReturnCodeKeys.E010;
        }
        //移除一个注册节点的任务
        try {
            TaskQueues.addHangUpTask(taskId, consumption);
        }catch (Exception e){
            //异常
            return ReturnCodeKeys.E000;
        }
        return ReturnCodeKeys.E008;
    }


    public int deleteTask(int taskId) {
        //任务编号小于0
        if(taskId<=0){
            return ReturnCodeKeys.E009;
        }
        //任务不存在
        if(!TaskQueues.getExitTaskIdSet().contains(taskId)){

            return ReturnCodeKeys.E012;
        }
        //移除一个任务
        try {
            TaskQueues.removeTask(taskId);
        }catch (Exception e){
            //异常
            return ReturnCodeKeys.E000;
        }
        return ReturnCodeKeys.E011;
    }


    public int scheduleTask(int threshold) {
        //小于0
        if(threshold<=0){
            return ReturnCodeKeys.E002;
        }
        //调度任务
        try {
            boolean scheduleSuccess = TaskQueues.schedule(threshold);
            if(scheduleSuccess){
                return ReturnCodeKeys.E013;
            }else{
                return ReturnCodeKeys.E014;
            }
        }catch (Exception e){
            //异常
            return ReturnCodeKeys.E014;
        }
    }


    public int queryTaskStatus(List<TaskInfo> tasks) {
        if(tasks==null){
            return ReturnCodeKeys.E016;
        }
        if(TaskQueues.exitTaskIdSet==null||TaskQueues.exitTaskIdSet.size()==0){
            return ReturnCodeKeys.E015;
        }
        for(Integer taskId: TaskQueues.exitTaskIdSet){
            TaskInfoInQueue taskInfoInQueue = TaskQueues.exitTaskMap.get(taskId);
            if(taskInfoInQueue==null||taskInfoInQueue.getTaskInfo()==null){
                continue;
            }
            //挂起则节点赋值为-1
            if(Objects.equals(taskInfoInQueue.getTaskStatus() ,TaskStatusEnums.HAND_UP.getCode())){
                taskInfoInQueue.getTaskInfo().setNodeId(-1);
            }
            tasks.add(taskInfoInQueue.getTaskInfo());
        }
        return ReturnCodeKeys.E015;
    }

}
