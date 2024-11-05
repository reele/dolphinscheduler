/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.server.master.runner.task.subworkflow.trigger;

import org.apache.dolphinscheduler.common.enums.CommandType;
import org.apache.dolphinscheduler.common.enums.Flag;
import org.apache.dolphinscheduler.common.enums.WarningType;
import org.apache.dolphinscheduler.common.enums.WorkflowExecutionStatus;
import org.apache.dolphinscheduler.common.utils.DateUtils;
import org.apache.dolphinscheduler.dao.entity.Command;
import org.apache.dolphinscheduler.dao.entity.WorkflowDefinition;
import org.apache.dolphinscheduler.dao.entity.WorkflowInstance;
import org.apache.dolphinscheduler.dao.utils.EnvironmentUtils;
import org.apache.dolphinscheduler.dao.utils.WorkerGroupUtils;
import org.apache.dolphinscheduler.server.master.engine.workflow.trigger.AbstractWorkflowTrigger;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.ObjectUtils;

import java.util.Date;

import org.springframework.stereotype.Component;

/**
 * Used to trigger the sub-workflow and generate the workflow instance.
 */
@Component
public class SubWorkflowTrigger
        extends
            AbstractWorkflowTrigger<SubWorkflowTriggerRequest, SubWorkflowTriggerResponse> {

    @Override
    protected WorkflowInstance constructWorkflowInstance(final SubWorkflowTriggerRequest subWorkflowTriggerRequest) {
        final CommandType commandType = CommandType.START_PROCESS;
        final Long workflowCode = subWorkflowTriggerRequest.getWorkflowDefinitionCode();
        final Integer workflowVersion = subWorkflowTriggerRequest.getWorkflowDefinitionVersion();
        final WorkflowDefinition workflowDefinition = getProcessDefinition(workflowCode, workflowVersion);

        final WorkflowInstance workflowInstance = new WorkflowInstance();
        workflowInstance.setWorkflowDefinitionCode(workflowDefinition.getCode());
        workflowInstance.setWorkflowDefinitionVersion(workflowDefinition.getVersion());
        workflowInstance.setProjectCode(workflowDefinition.getProjectCode());
        workflowInstance.setCommandType(commandType);
        workflowInstance.setStateWithDesc(WorkflowExecutionStatus.SUBMITTED_SUCCESS, commandType.name());
        workflowInstance.setRecovery(Flag.NO);
        workflowInstance.setScheduleTime(subWorkflowTriggerRequest.getScheduleTIme());
        workflowInstance.setStartTime(new Date());
        workflowInstance.setRestartTime(workflowInstance.getStartTime());
        workflowInstance.setRunTimes(1);
        workflowInstance.setName(String.join("-", workflowDefinition.getName(), DateUtils.getCurrentTimeStamp()));
        workflowInstance.setTaskDependType(subWorkflowTriggerRequest.getTaskDependType());
        workflowInstance.setFailureStrategy(subWorkflowTriggerRequest.getFailureStrategy());
        workflowInstance.setWarningType(
                ObjectUtils.defaultIfNull(subWorkflowTriggerRequest.getWarningType(), WarningType.NONE));
        workflowInstance.setWarningGroupId(subWorkflowTriggerRequest.getWarningGroupId());
        workflowInstance.setExecutorId(subWorkflowTriggerRequest.getUserId());
        workflowInstance.setExecutorName(getExecutorUser(subWorkflowTriggerRequest.getUserId()).getUserName());
        workflowInstance.setTenantCode(subWorkflowTriggerRequest.getTenantCode());
        workflowInstance.setIsSubWorkflow(Flag.YES);
        workflowInstance.addHistoryCmd(commandType);
        workflowInstance.setWorkflowInstancePriority(subWorkflowTriggerRequest.getWorkflowInstancePriority());
        workflowInstance.setWorkerGroup(
                WorkerGroupUtils.getWorkerGroupOrDefault(subWorkflowTriggerRequest.getWorkerGroup()));
        workflowInstance.setEnvironmentCode(
                EnvironmentUtils.getEnvironmentCodeOrDefault(subWorkflowTriggerRequest.getEnvironmentCode()));
        workflowInstance.setTimeout(workflowDefinition.getTimeout());
        workflowInstance.setDryRun(subWorkflowTriggerRequest.getDryRun().getCode());
        workflowInstance.setTestFlag(subWorkflowTriggerRequest.getTestFlag().getCode());
        return workflowInstance;
    }

    @Override
    protected Command constructTriggerCommand(final SubWorkflowTriggerRequest subWorkflowTriggerRequest,
                                              final WorkflowInstance workflowInstance) {
        throw new NotImplementedException();
    }

    @Override
    protected SubWorkflowTriggerResponse onTriggerSuccess(WorkflowInstance workflowInstance) {
        throw new NotImplementedException();
    }

}
