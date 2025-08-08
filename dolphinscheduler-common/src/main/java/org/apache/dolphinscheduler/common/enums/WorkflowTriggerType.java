package org.apache.dolphinscheduler.common.enums;

import com.baomidou.mybatisplus.annotation.EnumValue;
import lombok.Getter;

@Getter
public enum WorkflowTriggerType {

    MANUAL(0, "manual start workflow"),
    SCHEDULE(1, "schedule workflow"),
    BACKFILL(2, "backfill workflow");

    @EnumValue
    private final int code;

    private final String desc;

    WorkflowTriggerType(int code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    @Override
    public String toString() {
        return name();
    }
}
