/*
 * Copyright (c) 2023 OceanBase.
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

import java.io.File;
import java.lang.ProcessBuilder.Redirect;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceStability.Evolving;

import com.oceanbase.odc.common.util.StringUtils;
import com.oceanbase.odc.common.util.SystemUtils;
import com.oceanbase.odc.service.task.caller.DefaultExecutorIdentifier;
import com.oceanbase.odc.service.task.caller.ExecutorIdentifier;
import com.oceanbase.odc.service.task.caller.ExecutorIdentifierParser;
import com.oceanbase.odc.service.task.caller.ExecutorProcessBuilderFactory;
import com.oceanbase.odc.service.task.caller.JobContext;
import com.oceanbase.odc.service.task.caller.ProcessConfig;
import com.oceanbase.odc.service.task.constants.JobEnvKeyConstants;
import com.oceanbase.odc.service.task.exception.JobException;
import com.oceanbase.odc.service.task.supervisor.endpoint.ExecutorEndpoint;
import com.oceanbase.odc.service.task.supervisor.endpoint.SupervisorEndpoint;
import com.oceanbase.odc.service.task.util.JobUtils;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * submit a task context and return a executor endpoint in local mode
 * 
 * @author longpeng.zlp
 * @date 2024/10/28 10:55
 */
@Slf4j
@Evolving
public class TaskSupervisor {
    public static final String COMMAND_PROTOCOL_NAME = "command";
    @Getter
    private final SupervisorEndpoint supervisorEndpoint;

    public TaskSupervisor(SupervisorEndpoint supervisorEndpoint) {
        this.supervisorEndpoint = supervisorEndpoint;
    }

    /**
     * start task with given parameters
     * 
     * @param context
     * @param processConfig
     * @return
     */
    public ExecutorEndpoint startTask(JobContext context, ProcessConfig processConfig) throws JobException {
        String executorName = JobUtils.generateExecutorName(context.getJobIdentity());
        String portString = tryGenerateListenPortToEnv(processConfig);
        ProcessBuilder pb = new ExecutorProcessBuilderFactory().getProcessBuilder(
                processConfig, context.getJobIdentity().getId(), executorName);
        pb.redirectErrorStream(true);
        pb.redirectOutput(Redirect.appendTo(new File("process-call.log")));
        Process process;
        try {
            process = pb.start();
        } catch (Exception ex) {
            throw new JobException("Start process failed.", ex);
        }

        long pid = SystemUtils.getProcessPid(process);
        if (pid == -1) {
            process.destroyForcibly();
            throw new JobException("Get pid failed, job id={0} ", context.getJobIdentity().getId());
        }

        boolean isProcessRunning =
                SystemUtils.isProcessRunning(pid, JobUtils.generateExecutorSelectorOnProcess(executorName));

        if (!isProcessRunning) {
            process.destroyForcibly();
            throw new JobException("Start process failed, not process found, pid={0},executorName={1}.",
                    pid, executorName);
        }

        // set process id as namespace
        ExecutorIdentifier executorIdentifier = DefaultExecutorIdentifier.builder().host(supervisorEndpoint.getHost())
                .port(Integer.parseInt(portString))
                .namespace(pid + "")
                .executorName(executorName).build();
        return new ExecutorEndpoint(COMMAND_PROTOCOL_NAME, supervisorEndpoint.getHost(), supervisorEndpoint.getPort(),
                portString, executorIdentifier.toString());
    }

    /**
     * generate listen port for process if is pull mode
     * 
     * @param processConfig
     * @return
     */
    protected String tryGenerateListenPortToEnv(ProcessConfig processConfig) {
        String reportEnabled = getValue(processConfig.getEnvironments(), JobEnvKeyConstants.REPORT_ENABLED);
        // enable report mode, use push mode
        if (!StringUtils.equalsIgnoreCase(reportEnabled, "false")) {
            log.info("task run in push mode, port allocate not needed");
            return "-1";
        }
        // use pull mode, detect if port is given
        String givenPort = getValue(processConfig.getEnvironments(), JobEnvKeyConstants.ODC_EXECUTOR_PORT);
        if (null != givenPort && Integer.parseInt(givenPort) != 0) {
            log.info("task run in pull mode, allocatedPort = {}", givenPort);
            return givenPort;
        }
        // port not valid, fill it
        int detectPort = PortDetector.getInstance().getPort();
        processConfig.getEnvironments().put(JobEnvKeyConstants.ODC_EXECUTOR_PORT, String.valueOf(detectPort));
        log.info("task run in pull mode, port not given, allocatePort={}", detectPort);
        return String.valueOf(detectPort);
    }

    protected String getValue(Map<String, String> map, String key) {
        if (null == map) {
            return null;
        }
        return map.get(key);
    }

    /**
     * stop task
     * 
     * @param jobContext
     */
    public boolean stopTask(ExecutorEndpoint executorEndpoint, JobContext jobContext) {
        return true;
    }

    /**
     * modify a task without stop it
     * 
     * @param executorEndpoint
     * @param jobContext
     */
    public boolean modifyTask(ExecutorEndpoint executorEndpoint, JobContext jobContext) {
        return true;
    }

    /**
     * finish a task, it will not be started again
     * 
     * @param executorEndpoint
     * @param jobContext
     */
    public boolean finishTask(ExecutorEndpoint executorEndpoint, JobContext jobContext) throws JobException {
        ExecutorIdentifier executorIdentifier = getExecutorIdentifier(executorEndpoint);
        // kill process on this machine
        if (isTaskAlive(executorIdentifier)) {
            long pid = Long.parseLong(executorIdentifier.getNamespace());
            log.info("Found process, try kill it, pid={}.", pid);
            // first update destroy time, second destroy executor.
            // if executor failed update will be rollback, ensure distributed transaction atomicity.
            doDestroyInternal(executorIdentifier);
        }
        return true;
    }

    protected void doDestroyInternal(ExecutorIdentifier identifier) throws JobException {
        long pid = Long.parseLong(identifier.getNamespace());
        boolean result = SystemUtils.killProcessByPid(pid);
        if (result) {
            log.info("Destroy succeed by kill process, executorIdentifier={},  pid={}", identifier, pid);
        } else {
            throw new JobException(
                    "Destroy executor failed by kill process, identifier={0}, pid{1}=", identifier, pid);
        }
    }

    public boolean isTaskAlive(ExecutorIdentifier identifier) {
        long pid = Long.parseLong(identifier.getNamespace());
        boolean result = SystemUtils.isProcessRunning(pid,
                JobUtils.generateExecutorSelectorOnProcess(identifier.getExecutorName()));
        if (result) {
            log.info("Found executor by identifier, identifier={}", identifier);
        } else {
            log.warn("Not found executor by identifier, identifier={}", identifier);
        }
        return result;
    }

    protected ExecutorIdentifier getExecutorIdentifier(ExecutorEndpoint executorEndpoint) {
        return ExecutorIdentifierParser.parser(executorEndpoint.getIdentifier());
    }
}
