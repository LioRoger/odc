/*
 * Copyright (c) 2024 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.oceanbase.odc.service.task.supervisor.proxy;

import java.io.IOException;

import com.oceanbase.odc.common.json.JsonUtils;
import com.oceanbase.odc.service.task.caller.JobContext;
import com.oceanbase.odc.service.task.caller.ProcessConfig;
import com.oceanbase.odc.service.task.exception.JobException;
import com.oceanbase.odc.service.task.supervisor.TaskSupervisor;
import com.oceanbase.odc.service.task.supervisor.endpoint.ExecutorEndpoint;
import com.oceanbase.odc.service.task.supervisor.endpoint.SupervisorEndpoint;
import com.oceanbase.odc.service.task.supervisor.protocol.TaskCommandSender;
import com.oceanbase.odc.service.task.supervisor.proxy.RemoteTaskSupervisorProxy;
import com.oceanbase.odc.service.task.supervisor.proxy.TaskSupervisorProxy;
import com.oceanbase.odc.service.task.util.TaskExecutorClient;

import lombok.extern.slf4j.Slf4j;

/**
 * command executor to route task command local impl
 * 
 * @author longpeng.zlp
 * @date 2024/10/29 11:43
 */
@Slf4j
public class LocalTaskSupervisorProxy implements TaskSupervisorProxy {
    // command sender to task executor
    private final TaskExecutorClient        taskExecutorClient;
    private final TaskSupervisor            taskSupervisor;
    private final RemoteTaskSupervisorProxy remoteTaskSupervisorProxy;
    private final SupervisorEndpoint        localEndPoint;

    public LocalTaskSupervisorProxy(TaskExecutorClient taskExecutorClient, SupervisorEndpoint supervisorEndpoint, String mainClassName) {
        this.taskExecutorClient = taskExecutorClient;
        this.localEndPoint = supervisorEndpoint;
        log.info("LocalTaskSupervisorProxy start with endpoint={}", supervisorEndpoint);
        remoteTaskSupervisorProxy = new RemoteTaskSupervisorProxy(new TaskCommandSender(), taskExecutorClient);
        taskSupervisor = new TaskSupervisor(supervisorEndpoint, mainClassName);
    }

    @Override
    public ExecutorEndpoint startTask(SupervisorEndpoint supervisorEndpoint, JobContext jobContext,
            ProcessConfig processConfig) throws JobException, IOException {
        if (isLocalCommandCall(supervisorEndpoint)) {
            log.info("local call start task, supervisorEndpoint={}, jobContext={}, processConfig={}",
                    supervisorEndpoint, jobContext, processConfig);
            return taskSupervisor.startTask(jobContext, processConfig);
        } else {
            log.info("remote call start task, supervisorEndpoint={}, jobContext={}, processConfig={}",
                    supervisorEndpoint, jobContext, processConfig);
            return remoteTaskSupervisorProxy.startTask(supervisorEndpoint, jobContext, processConfig);
        }
    }

    @Override
    public boolean stopTask(SupervisorEndpoint supervisorEndpoint, ExecutorEndpoint executorEndpoint,
            JobContext jobContext) throws JobException {
        if (null == executorEndpoint) {
            throw new JobException("empty executor endpoint to stop");
        }
        if (isLocalCommandCall(supervisorEndpoint)) {
            log.info("local call stop task, supervisorEndpoint={}, executorEndpoint={}, jobContext={}",
                    supervisorEndpoint, executorEndpoint, jobContext);
            return taskSupervisor.stopTask(executorEndpoint, jobContext);
        } else {
            log.info("remote call stop task, supervisorEndpoint={}, executorEndpoint={}, jobContext={}",
                    supervisorEndpoint, executorEndpoint, jobContext);
            return remoteTaskSupervisorProxy.stopTask(supervisorEndpoint, executorEndpoint, jobContext);
        }
    }

    /**
     * modify task use
     * @param supervisorEndpoint
     * @param executorEndpoint
     * @param jobContext
     * @return
     * @throws JobException
     */
    public boolean modifyTask(SupervisorEndpoint supervisorEndpoint, ExecutorEndpoint executorEndpoint,
        JobContext jobContext) throws JobException {
        taskExecutorClient.modifyJobParameters(TaskSupervisorProxy.getExecutorIdentifierByExecutorEndpoint(executorEndpoint), jobContext.getJobIdentity(),
            JsonUtils.toJson(jobContext.getJobParameters()));
        return true;
    }


    @Override
    public boolean isTaskAlive(SupervisorEndpoint supervisorEndpoint, ExecutorEndpoint executorEndpoint,
            JobContext jobContext) throws JobException, IOException {
        if (isLocalCommandCall(supervisorEndpoint)) {
            log.info("local call canBeFinished task, supervisorEndpoint={}, executorEndpoint={}, jobContext={}",
                    supervisorEndpoint, executorEndpoint, jobContext);
            return taskSupervisor.isTaskAlive(TaskSupervisor.getExecutorIdentifier(executorEndpoint));
        } else {
            log.info("remote call canBeFinished task, supervisorEndpoint={}, executorEndpoint={}, jobContext={}",
                    supervisorEndpoint, executorEndpoint, jobContext);
            return remoteTaskSupervisorProxy.isTaskAlive(supervisorEndpoint, executorEndpoint, jobContext);
        }
    }

    @Override
    public boolean isSupervisorAlive(SupervisorEndpoint supervisorEndpoint) {
        if (isLocalCommandCall(supervisorEndpoint)) {
            log.info("local call isSupervisorAlive task, supervisorEndpoint={}", supervisorEndpoint);
            return true;
        } else {
            log.info("remote call isSupervisorAlive task, supervisorEndpoint={}", supervisorEndpoint);
            return remoteTaskSupervisorProxy.isSupervisorAlive(supervisorEndpoint);
        }
    }

    protected boolean isLocalCommandCall(SupervisorEndpoint supervisorEndpoint) {
        if (null == supervisorEndpoint) {
            throw new IllegalStateException("end point must be given for task supervisor proxy");
        }
        return supervisorEndpoint.equals(SupervisorEndpoint.SELF_ENDPOINT) || supervisorEndpoint.equals(localEndPoint);
    }
}
