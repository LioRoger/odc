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

package com.oceanbase.odc.service.task.supervisor;


import com.oceanbase.odc.service.task.caller.ExecutorIdentifier;
import com.oceanbase.odc.service.task.caller.JobContext;
import com.oceanbase.odc.service.task.caller.ProcessConfig;
import com.oceanbase.odc.service.task.enums.JobCallerAction;
import com.oceanbase.odc.service.task.exception.JobException;
import com.oceanbase.odc.service.task.listener.JobCallerEvent;
import com.oceanbase.odc.service.task.supervisor.endpoint.ExecutorEndpoint;
import com.oceanbase.odc.service.task.supervisor.endpoint.SupervisorEndpoint;
import com.oceanbase.odc.service.task.supervisor.proxy.LocalTaskSupervisorProxy;

import lombok.extern.slf4j.Slf4j;

/**
 * job caller for task operation
 * @author longpeng.zlp
 * @date 2024/11/28 14:49
 */
@Slf4j
public class TaskSupervisorJobCaller {
    // event listener
    private final JobEventHandler          jobEventHandler;
    // super visor command
    private final LocalTaskSupervisorProxy taskSupervisorProxy;
    
    public TaskSupervisorJobCaller(JobEventHandler jobEventHandler,
        LocalTaskSupervisorProxy taskSupervisorProxy) {
        this.jobEventHandler = jobEventHandler;
        this.taskSupervisorProxy = taskSupervisorProxy;
    }

    public ExecutorEndpoint startTask(SupervisorEndpoint supervisorEndpoint, JobContext jobContext,
        ProcessConfig processConfig) throws JobException {
        ExecutorIdentifier executorIdentifier = null;
        ExecutorEndpoint executorEndpoint = null;
        try {
            // do start process
            jobEventHandler.beforeStartJob(jobContext);
            executorEndpoint = taskSupervisorProxy.startTask(supervisorEndpoint, jobContext, processConfig);
            jobEventHandler.afterStartJob(executorIdentifier, jobContext);
            // send success event
            log.info("Start job succeed, jobId={}.", jobContext.getJobIdentity().getId());
            jobEventHandler.onNewEvent(new JobCallerEvent(jobContext.getJobIdentity(), JobCallerAction.START, true, executorIdentifier, null));
            return executorEndpoint;
        } catch (Exception e) {
            // try roll back
            try {
                stopTask(supervisorEndpoint, executorEndpoint, jobContext);
            } catch (Throwable ex) {
                log.warn("Start job failed, process stop failed too", ex);
            }
            // send failed event
            jobEventHandler.onNewEvent(new JobCallerEvent(jobContext.getJobIdentity(), JobCallerAction.START, false, e));
            throw new JobException("Start job failed", e);
        }
    }

    public boolean stopTask(SupervisorEndpoint supervisorEndpoint, ExecutorEndpoint executorEndpoint, JobContext jobContext) throws Exception {
        try {
            // try stop
            taskSupervisorProxy.stopTask(supervisorEndpoint, executorEndpoint, jobContext);
            log.info("Stop job successfully, jobId={}.", jobContext.getJobIdentity().getId());
            jobEventHandler.onNewEvent(new JobCallerEvent(jobContext.getJobIdentity(), JobCallerAction.STOP, true, null));
            return true;
        } catch (Exception e) {
            // handle stop exception
            jobEventHandler.onNewEvent(new JobCallerEvent(jobContext.getJobIdentity(), JobCallerAction.STOP, false, e));
            throw new JobException("job be stop failed, jobId={0}.", e, jobContext.getJobIdentity().getId());
        }
    }

    public boolean modify(SupervisorEndpoint supervisorEndpoint, ExecutorEndpoint executorEndpoint, JobContext jobContext)
        throws JobException {
        return taskSupervisorProxy.modifyTask(supervisorEndpoint, executorEndpoint, jobContext);
    }

    /**
     * it's only db related operation
     * TODO(lx):it will be removed out of this class
     * @param jobContext
     * @return
     */
    public boolean finish(SupervisorEndpoint supervisorEndpoint, ExecutorEndpoint executorEndpoint, JobContext jobContext)
        throws JobException {
        if (null == executorEndpoint) {
            log.info("job finished success, it's not created yet");
        } else {
            log.info("try finished job, executorEndpoint={}", executorEndpoint);
            // target supervisor is down
            if (!taskSupervisorProxy.isSupervisorAlive(supervisorEndpoint)) {
                jobEventHandler.finishFailed(TaskSupervisor.getExecutorIdentifier(executorEndpoint), jobContext);
            } else {
                taskSupervisorProxy.stopTask(supervisorEndpoint, executorEndpoint, jobContext);
            }
        }
        jobEventHandler.afterFinished(null, jobContext);
        return true;
    }

    /**
     * for supervisor agent, it will always be true, cause command will be routed to right agent
     * @param jobContext
     * @return
     */
    public boolean canBeFinish(SupervisorEndpoint supervisorEndpoint, ExecutorEndpoint executorEndpoint, JobContext jobContext)
        throws JobException {
        return true;
    }
}
