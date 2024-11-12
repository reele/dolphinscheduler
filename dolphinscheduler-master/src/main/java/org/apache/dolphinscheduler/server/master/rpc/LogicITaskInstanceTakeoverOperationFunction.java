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

package org.apache.dolphinscheduler.server.master.rpc;

import lombok.extern.slf4j.Slf4j;
import org.apache.dolphinscheduler.extract.master.transportor.LogicTaskTakeoverRequest;
import org.apache.dolphinscheduler.extract.master.transportor.LogicTaskTakeoverResponse;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.enums.TaskExecutionStatus;
import org.apache.dolphinscheduler.plugin.task.api.utils.LogUtils;
import org.apache.dolphinscheduler.server.master.runner.execute.MasterTaskExecutionContextHolder;
import org.apache.dolphinscheduler.server.master.runner.execute.MasterTaskExecutor;
import org.apache.dolphinscheduler.server.master.runner.execute.MasterTaskExecutorFactoryBuilder;
import org.apache.dolphinscheduler.server.master.runner.execute.MasterTaskExecutorThreadPoolManager;
import org.apache.dolphinscheduler.server.master.runner.message.LogicTaskInstanceExecutionEventSenderManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class LogicITaskInstanceTakeoverOperationFunction
        implements
            ITaskInstanceOperationFunction<LogicTaskTakeoverRequest, LogicTaskTakeoverResponse> {

    @Autowired
    private MasterTaskExecutorFactoryBuilder masterTaskExecutorFactoryBuilder;

    @Autowired
    private MasterTaskExecutorThreadPoolManager masterTaskExecutorThreadPool;

    @Autowired
    private LogicTaskInstanceExecutionEventSenderManager logicTaskInstanceExecutionEventSenderManager;

    @Override
    public LogicTaskTakeoverResponse operate(LogicTaskTakeoverRequest taskTakeoverRequest) {
        log.info("Received dispatchLogicTask request: {}", taskTakeoverRequest);
        TaskExecutionContext taskExecutionContext = taskTakeoverRequest.getTaskExecutionContext();
        try {
            final int taskInstanceId = taskExecutionContext.getTaskInstanceId();
            final int workflowInstanceId = taskExecutionContext.getWorkflowInstanceId();
            final String taskInstanceName = taskExecutionContext.getTaskName();

            taskExecutionContext.setLogPath(LogUtils.getTaskInstanceLogFullPath(taskExecutionContext));

            LogUtils.setWorkflowAndTaskInstanceIDMDC(workflowInstanceId, taskInstanceId);
            LogUtils.setTaskInstanceLogFullPathMDC(taskExecutionContext.getLogPath());

            MasterTaskExecutionContextHolder.putTaskExecutionContext(taskExecutionContext);

            final MasterTaskExecutor masterTaskExecutor = masterTaskExecutorFactoryBuilder
                    .createMasterTaskExecutorFactory(taskExecutionContext.getTaskType())
                    .createMasterTaskExecutor(taskExecutionContext);

            if (masterTaskExecutorThreadPool.takeoverMasterTaskExecutor(masterTaskExecutor)) {
                log.info("Takeover LogicTask: {} to MasterTaskExecutorThreadPool success", taskInstanceName);
                return LogicTaskTakeoverResponse.success(taskInstanceId);
            } else {
                log.error("Takeover LogicTask: {} to MasterTaskExecutorThreadPool failed", taskInstanceName);
                return LogicTaskTakeoverResponse.failed(taskInstanceId, "MasterTaskExecutorThreadPool is full");
            }
        } finally {
            LogUtils.removeWorkflowAndTaskInstanceIdMDC();
            LogUtils.removeTaskInstanceLogFullPathMDC();
        }
    }
}
