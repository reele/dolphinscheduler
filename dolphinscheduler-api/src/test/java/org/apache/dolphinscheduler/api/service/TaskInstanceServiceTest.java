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

package org.apache.dolphinscheduler.api.service;

import static org.apache.dolphinscheduler.api.AssertionsHelper.assertThrowsServiceException;
import static org.apache.dolphinscheduler.api.constants.ApiFuncIdentificationConstant.FORCED_SUCCESS;
import static org.apache.dolphinscheduler.api.constants.ApiFuncIdentificationConstant.TASK_INSTANCE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.exceptions.ServiceException;
import org.apache.dolphinscheduler.api.service.impl.ProjectServiceImpl;
import org.apache.dolphinscheduler.api.service.impl.TaskInstanceServiceImpl;
import org.apache.dolphinscheduler.api.utils.Result;
import org.apache.dolphinscheduler.common.constants.Constants;
import org.apache.dolphinscheduler.common.enums.TaskExecuteType;
import org.apache.dolphinscheduler.common.enums.UserType;
import org.apache.dolphinscheduler.common.enums.WorkflowExecutionStatus;
import org.apache.dolphinscheduler.common.utils.DateUtils;
import org.apache.dolphinscheduler.dao.entity.Project;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.dao.entity.User;
import org.apache.dolphinscheduler.dao.entity.WorkflowInstance;
import org.apache.dolphinscheduler.dao.mapper.ProjectMapper;
import org.apache.dolphinscheduler.dao.mapper.TaskDefinitionMapper;
import org.apache.dolphinscheduler.dao.mapper.TaskInstanceMapper;
import org.apache.dolphinscheduler.dao.repository.TaskInstanceDao;
import org.apache.dolphinscheduler.dao.repository.WorkflowInstanceDao;
import org.apache.dolphinscheduler.plugin.task.api.enums.TaskExecutionStatus;
import org.apache.dolphinscheduler.service.process.ProcessService;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;

/**
 * task instance service test
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class TaskInstanceServiceTest {

    @InjectMocks
    private TaskInstanceServiceImpl taskInstanceService;

    @Mock
    ProjectMapper projectMapper;

    @Mock
    ProjectServiceImpl projectService;

    @Mock
    ProcessService processService;

    @Mock
    TaskInstanceMapper taskInstanceMapper;

    @Mock
    UsersService usersService;

    @Mock
    TaskDefinitionMapper taskDefinitionMapper;

    @Mock
    TaskInstanceDao taskInstanceDao;
    @Mock
    WorkflowInstanceDao workflowInstanceDao;

    @Test
    public void queryTaskListPaging() {
        long projectCode = 1L;
        User loginUser = getAdminUser();
        Project project = getProject(projectCode);
        Map<String, Object> result = new HashMap<>();
        putMsg(result, Status.PROJECT_NOT_FOUND, projectCode);

        // project auth fail
        doThrow(new ServiceException()).when(projectService).checkProjectAndAuthThrowException(loginUser, projectCode,
                TASK_INSTANCE);
        Assertions.assertThrows(ServiceException.class, () -> taskInstanceService.queryTaskListPaging(loginUser,
                projectCode,
                0,
                "",
                "",
                "",
                null,
                "test_user",
                "2019-02-26 19:48:00",
                "2019-02-26 19:48:22",
                "",
                null,
                "",
                TaskExecuteType.BATCH,
                1,
                20));

        // data parameter check
        putMsg(result, Status.SUCCESS, projectCode);
        when(projectMapper.queryByCode(projectCode)).thenReturn(project);
        when(projectService.checkProjectAndAuth(loginUser, project, projectCode, TASK_INSTANCE)).thenReturn(result);
        Assertions.assertThrows(ServiceException.class, () -> taskInstanceService.queryTaskListPaging(loginUser,
                projectCode,
                1,
                "",
                "",
                "",
                null,
                "test_user",
                "20200101 00:00:00",
                "2020-01-02 00:00:00",
                "",
                TaskExecutionStatus.SUCCESS,
                "192.168.xx.xx",
                TaskExecuteType.BATCH,
                1,
                20));

        // project
        putMsg(result, Status.SUCCESS, projectCode);
        Date start = DateUtils.stringToDate("2020-01-01 00:00:00");
        Date end = DateUtils.stringToDate("2020-01-02 00:00:00");
        WorkflowInstance workflowInstance = getProcessInstance();
        TaskInstance taskInstance = getTaskInstance();
        List<TaskInstance> taskInstanceList = new ArrayList<>();
        Page<TaskInstance> pageReturn = new Page<>(1, 10);
        taskInstanceList.add(taskInstance);
        pageReturn.setRecords(taskInstanceList);
        doNothing().when(projectService).checkProjectAndAuthThrowException(loginUser, projectCode, TASK_INSTANCE);
        when(usersService.queryUser(loginUser.getId())).thenReturn(loginUser);
        when(usersService.getUserIdByName(loginUser.getUserName())).thenReturn(loginUser.getId());
        when(taskInstanceMapper.queryTaskInstanceListPaging(
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any()))
                        .thenReturn(pageReturn);
        when(usersService.queryUser(workflowInstance.getExecutorId())).thenReturn(loginUser);
        when(processService.findWorkflowInstanceDetailById(taskInstance.getWorkflowInstanceId()))
                .thenReturn(Optional.of(workflowInstance));

        Result successRes = taskInstanceService.queryTaskListPaging(loginUser,
                projectCode,
                1,
                "",
                "",
                "",
                null,
                "test_user",
                "2020-01-01 00:00:00",
                "2020-01-02 00:00:00",
                "",
                TaskExecutionStatus.SUCCESS,
                "192.168.xx.xx",
                TaskExecuteType.BATCH,
                1,
                20);
        Assertions.assertEquals(Status.SUCCESS.getCode(), (int) successRes.getCode());

        // executor name empty
        when(taskInstanceMapper.queryTaskInstanceListPaging(
                Mockito.any(Page.class), eq(project.getCode()), eq(1),
                eq(""), eq(""), eq(""), eq(null),
                eq(""), Mockito.any(), eq("192.168.xx.xx"), eq(TaskExecuteType.BATCH), eq(start), eq(end)))
                        .thenReturn(pageReturn);
        Result executorEmptyRes = taskInstanceService.queryTaskListPaging(loginUser, projectCode, 1, "", "", "",
                null, "", "2020-01-01 00:00:00", "2020-01-02 00:00:00", "", TaskExecutionStatus.SUCCESS,
                "192.168.xx.xx",
                TaskExecuteType.BATCH, 1, 20);
        Assertions.assertEquals(Status.SUCCESS.getCode(), (int) executorEmptyRes.getCode());

        // executor null
        when(usersService.queryUser(loginUser.getId())).thenReturn(null);
        when(usersService.getUserIdByName(loginUser.getUserName())).thenReturn(-1);

        Result executorNullRes = taskInstanceService.queryTaskListPaging(loginUser, projectCode, 1, "", "", "",
                null, "test_user", "2020-01-01 00:00:00", "2020-01-02 00:00:00", "", TaskExecutionStatus.SUCCESS,
                "192.168.xx.xx", TaskExecuteType.BATCH, 1, 20);
        Assertions.assertEquals(Status.SUCCESS.getCode(), (int) executorNullRes.getCode());

        // start/end date null
        when(taskInstanceMapper.queryTaskInstanceListPaging(Mockito.any(Page.class), eq(project.getCode()), eq(1),
                eq(""), eq(""), eq(""), eq(null),
                eq(""), Mockito.any(), eq("192.168.xx.xx"), eq(TaskExecuteType.BATCH), any(), any()))
                        .thenReturn(pageReturn);
        Result executorNullDateRes = taskInstanceService.queryTaskListPaging(loginUser, projectCode, 1, "", "", "",
                null, "", null, null, "", TaskExecutionStatus.SUCCESS, "192.168.xx.xx", TaskExecuteType.BATCH, 1, 20);
        Assertions.assertEquals(Status.SUCCESS.getCode(), (int) executorNullDateRes.getCode());

        // start date error format
        when(taskInstanceMapper.queryTaskInstanceListPaging(Mockito.any(Page.class), eq(project.getCode()), eq(1),
                eq(""), eq(""), eq(""), eq(null),
                eq(""), Mockito.any(), eq("192.168.xx.xx"), eq(TaskExecuteType.BATCH), any(), any()))
                        .thenReturn(pageReturn);

        Assertions.assertThrows(ServiceException.class, () -> taskInstanceService.queryTaskListPaging(
                loginUser,
                projectCode,
                1,
                "",
                "",
                "",
                null,
                "",
                "error date",
                null,
                "",
                TaskExecutionStatus.SUCCESS,
                "192.168.xx.xx",
                TaskExecuteType.BATCH,
                1,
                20));

        Assertions.assertThrows(ServiceException.class, () -> taskInstanceService.queryTaskListPaging(
                loginUser,
                projectCode,
                1,
                "",
                "",
                "",
                null,
                "",
                null,
                "error date",
                "",
                TaskExecutionStatus.SUCCESS,
                "192.168.xx.xx",
                TaskExecuteType.BATCH,
                1,
                20));
    }

    /**
     * get Mock Admin User
     *
     * @return admin user
     */
    private User getAdminUser() {
        User loginUser = new User();
        loginUser.setId(-1);
        loginUser.setUserName("admin");
        loginUser.setUserType(UserType.GENERAL_USER);
        return loginUser;
    }

    /**
     * get mock Project
     *
     * @param projectCode projectCode
     * @return Project
     */
    private Project getProject(long projectCode) {
        Project project = new Project();
        project.setCode(projectCode);
        project.setId(1);
        project.setName("project_test1");
        project.setUserId(1);
        return project;
    }

    /**
     * get Mock process instance
     *
     * @return process instance
     */
    private WorkflowInstance getProcessInstance() {
        WorkflowInstance workflowInstance = new WorkflowInstance();
        workflowInstance.setId(1);
        workflowInstance.setName("test_process_instance");
        workflowInstance.setStartTime(new Date());
        workflowInstance.setEndTime(new Date());
        workflowInstance.setExecutorId(-1);
        return workflowInstance;
    }

    /**
     * get Mock task instance
     *
     * @return task instance
     */
    private TaskInstance getTaskInstance() {
        TaskInstance taskInstance = new TaskInstance();
        taskInstance.setId(1);
        taskInstance.setProjectCode(1L);
        taskInstance.setName("test_task_instance");
        taskInstance.setStartTime(new Date());
        taskInstance.setEndTime(new Date());
        taskInstance.setExecutorId(-1);
        taskInstance.setTaskExecuteType(TaskExecuteType.BATCH);
        taskInstance.setState(TaskExecutionStatus.SUBMITTED_SUCCESS);
        return taskInstance;
    }

    private void putMsg(Map<String, Object> result, Status status, Object... statusParams) {
        result.put(Constants.STATUS, status);
        if (statusParams != null && statusParams.length > 0) {
            result.put(Constants.MSG, MessageFormat.format(status.getMsg(), statusParams));
        } else {
            result.put(Constants.MSG, status.getMsg());
        }
    }

    @Test
    public void testForceTaskSuccess_withNoPermission() {
        User user = getAdminUser();
        TaskInstance task = getTaskInstance();
        doThrow(new ServiceException(Status.USER_NO_OPERATION_PROJECT_PERM)).when(projectService)
                .checkProjectAndAuthThrowException(user, task.getProjectCode(), FORCED_SUCCESS);
        assertThrowsServiceException(Status.USER_NO_OPERATION_PROJECT_PERM,
                () -> taskInstanceService.forceTaskSuccess(user, task.getProjectCode(), task.getId()));
    }

    @Test
    public void testForceTaskSuccess_withTaskInstanceNotFound() {
        User user = getAdminUser();
        TaskInstance task = getTaskInstance();
        doNothing().when(projectService).checkProjectAndAuthThrowException(user, task.getProjectCode(), FORCED_SUCCESS);
        when(taskInstanceDao.queryOptionalById(task.getId())).thenReturn(Optional.empty());
        assertThrowsServiceException(Status.TASK_INSTANCE_NOT_FOUND,
                () -> taskInstanceService.forceTaskSuccess(user, task.getProjectCode(), task.getId()));
    }

    @Test
    public void testForceTaskSuccess_withWorkflowInstanceNotFound() {
        User user = getAdminUser();
        TaskInstance task = getTaskInstance();
        doNothing().when(projectService).checkProjectAndAuthThrowException(user, task.getProjectCode(), FORCED_SUCCESS);
        when(taskInstanceDao.queryOptionalById(task.getId())).thenReturn(Optional.of(task));
        when(workflowInstanceDao.queryOptionalById(task.getWorkflowInstanceId())).thenReturn(Optional.empty());

        assertThrowsServiceException(Status.WORKFLOW_INSTANCE_NOT_EXIST,
                () -> taskInstanceService.forceTaskSuccess(user, task.getProjectCode(), task.getId()));
    }

    @Test
    public void testForceTaskSuccess_withWorkflowInstanceNotFinished() {
        User user = getAdminUser();
        long projectCode = 1L;
        TaskInstance task = getTaskInstance();
        WorkflowInstance workflowInstance = getProcessInstance();
        workflowInstance.setState(WorkflowExecutionStatus.RUNNING_EXECUTION);
        doNothing().when(projectService).checkProjectAndAuthThrowException(user, projectCode, FORCED_SUCCESS);
        when(taskInstanceDao.queryOptionalById(task.getId())).thenReturn(Optional.of(task));
        when(workflowInstanceDao.queryOptionalById(task.getWorkflowInstanceId()))
                .thenReturn(Optional.of(workflowInstance));

        assertThrowsServiceException(
                "The workflow instance is not finished: " + workflowInstance.getState()
                        + " cannot force start task instance",
                () -> taskInstanceService.forceTaskSuccess(user, projectCode, task.getId()));
    }

    @Test
    public void testForceTaskSuccess_withTaskInstanceNotFinished() {
        User user = getAdminUser();
        TaskInstance task = getTaskInstance();
        WorkflowInstance workflowInstance = getProcessInstance();
        workflowInstance.setState(WorkflowExecutionStatus.FAILURE);
        doNothing().when(projectService).checkProjectAndAuthThrowException(user, task.getProjectCode(), FORCED_SUCCESS);
        when(taskInstanceDao.queryOptionalById(task.getId())).thenReturn(Optional.of(task));
        when(workflowInstanceDao.queryOptionalById(task.getWorkflowInstanceId()))
                .thenReturn(Optional.of(workflowInstance));

        assertThrowsServiceException(
                Status.TASK_INSTANCE_STATE_OPERATION_ERROR,
                () -> taskInstanceService.forceTaskSuccess(user, task.getProjectCode(), task.getId()));
    }

}
