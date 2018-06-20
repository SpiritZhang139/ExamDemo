package com.migu.schedule.constants;

/**
 * Created by spirit on 18/6/20.
 */
public enum TaskStatusEnums {
    HAND_UP(1,"挂起"),
    RUNNING(2,"运行"),
    END(3,"停止"),
    ;

    /**
     * 任务状态code
     */
    private int code;

    /**
     * 描述
     */
    private String desc;

    TaskStatusEnums(int code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public int getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }
}
