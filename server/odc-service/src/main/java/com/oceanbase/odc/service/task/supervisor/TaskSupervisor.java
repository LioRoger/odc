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

import com.oceanbase.odc.service.task.caller.JobContext;
import com.oceanbase.odc.service.task.caller.ProcessConfig;
import com.oceanbase.odc.service.task.supervisor.endpoint.ExecutorEndpoint;

/**
 * submit a task context and return a executor endpoint in local mode
 * @author longpeng.zlp
 * @date 2024/10/28 10:55
 */
public class TaskSupervisor {
    /**
     * start task with given parameters
     * @param jobContext
     * @param processConfig
     * @return
     */
    public ExecutorEndpoint startTask(JobContext jobContext, ProcessConfig processConfig) {
        return null;
    }

    /**
     * stop task
     * @param jobContext
     */
    public boolean stopTask(ExecutorEndpoint executorEndpoint, JobContext jobContext) {
        return true;
    }

    /**
     * modify a task without stop it
     * @param executorEndpoint
     * @param jobContext
     */
    public boolean modifyTask(ExecutorEndpoint executorEndpoint, JobContext jobContext) {
        return true;
    }

    /**
     * finish a task, it will not be started again
     * @param executorEndpoint
     * @param jobContext
     */
    public boolean finishTask(ExecutorEndpoint executorEndpoint, JobContext jobContext) {
        return true;
    }

    /**
     * compatible with old job caller
     * @param executorEndpoint
     * @param jobContext
     * @return
     */
    public boolean canBeFinished(ExecutorEndpoint executorEndpoint, JobContext jobContext) {
        return true;
    }
}
