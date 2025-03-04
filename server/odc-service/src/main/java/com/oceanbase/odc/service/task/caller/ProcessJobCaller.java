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

package com.oceanbase.odc.service.task.caller;

import static com.oceanbase.odc.service.task.constants.JobConstants.ODC_EXECUTOR_CANNOT_BE_DESTROYED;

import java.io.File;
import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.text.MessageFormat;
import java.util.Objects;
import java.util.Optional;

import com.fasterxml.jackson.core.type.TypeReference;
import com.oceanbase.odc.common.json.JsonUtils;
import com.oceanbase.odc.common.util.SystemUtils;
import com.oceanbase.odc.metadb.task.JobEntity;
import com.oceanbase.odc.service.common.response.OdcResult;
import com.oceanbase.odc.service.resource.ResourceID;
import com.oceanbase.odc.service.resource.ResourceLocation;
import com.oceanbase.odc.service.task.config.JobConfiguration;
import com.oceanbase.odc.service.task.config.JobConfigurationHolder;
import com.oceanbase.odc.service.task.enums.JobStatus;
import com.oceanbase.odc.service.task.exception.JobException;
import com.oceanbase.odc.service.task.schedule.JobIdentity;
import com.oceanbase.odc.service.task.util.HttpClientUtils;
import com.oceanbase.odc.service.task.util.JobUtils;

import lombok.extern.slf4j.Slf4j;

/**
 * @author yaobin
 * @date 2023-11-22
 * @since 4.2.4
 */
@Slf4j
public class ProcessJobCaller extends BaseJobCaller {
    private static final ResourceLocation LOCAL_RESOURCE_LOCATION =
            new ResourceLocation(SystemUtils.getLocalIpAddress(), ResourceIDUtil.DEFAULT_PROP_VALUE);

    private final ProcessConfig processConfig;

    public ProcessJobCaller(ProcessConfig processConfig) {
        this.processConfig = processConfig;
    }

    @Override
    public ExecutorInfo doStart(JobContext context) throws JobException {

        String executorName = JobUtils.generateExecutorName(context.getJobIdentity());
        ProcessBuilder pb = new ExecutorProcessBuilderFactory().getProcessBuilder(
                processConfig, context.getJobIdentity().getId(), executorName);
        log.info("start task with processConfig={}, env={}", JobUtils.toJson(processConfig),
                JsonUtils.toJson(pb.environment()));
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

        JobConfiguration jobConfiguration = JobConfigurationHolder.getJobConfiguration();
        String portString = Optional.ofNullable(jobConfiguration.getHostProperties().getPort())
                .orElse(DefaultExecutorIdentifier.DEFAULT_PORT + "");
        // set process id as namespace
        return new ExecutorInfo(new ResourceID(LOCAL_RESOURCE_LOCATION, "process", "processnamespace", executorName),
                DefaultExecutorIdentifier.builder().host(SystemUtils.getLocalIpAddress())
                        .port(Integer.parseInt(portString))
                        .namespace(pid + "")
                        .executorName(executorName).build());
    }

    @Override
    protected void doStop(JobIdentity ji) throws JobException {}

    @Override
    protected void doFinish(JobIdentity ji, ExecutorIdentifier ei, ResourceID resourceID)
            throws JobException {
        if (isExecutorExist(ei, resourceID)) {
            long pid = Long.parseLong(ei.getNamespace());
            log.info("Found process, try kill it, pid={}.", pid);
            // first update destroy time, second destroy executor.
            // if executor failed update will be rollback, ensure distributed transaction atomicity.
            updateExecutorDestroyed(ji);
            doDestroyInternal(ei);
            return;
        }

        JobConfiguration jobConfiguration = JobConfigurationHolder.getJobConfiguration();
        String portString = Optional.ofNullable(jobConfiguration.getHostProperties().getPort())
                .orElse(DefaultExecutorIdentifier.DEFAULT_PORT + "");
        if (SystemUtils.getLocalIpAddress().equals(ei.getHost()) && Objects.equals(portString, ei.getPort() + "")) {
            updateExecutorDestroyed(ji);
            return;
        }
        JobConfiguration configuration = JobConfigurationHolder.getJobConfiguration();
        JobEntity jobEntity = configuration.getTaskFrameworkService().find(ji.getId());
        if (!isOdcHealthy(ei.getHost(), ei.getPort())) {
            if (jobEntity.getStatus() == JobStatus.RUNNING) {
                // Cannot connect to target identifier,we cannot kill the process,
                // so we set job to FAILED and avoid two process running
                configuration.getTaskFrameworkService().updateStatusDescriptionByIdOldStatus(
                        ji.getId(), JobStatus.RUNNING, JobStatus.FAILED,
                        MessageFormat.format("Cannot connect to target odc server, jodId={0}, identifier={1}",
                                ji.getId(), ei));
            }
            updateExecutorDestroyed(ji);
            log.warn("Cannot connect to target odc server, set job to failed, jodId={}, identifier={}",
                    ji.getId(), ei);
            return;
        }
        throw new JobException(ODC_EXECUTOR_CANNOT_BE_DESTROYED +
                "Connect to target odc server succeed, but cannot destroy process,"
                + " may not on this machine, jodId={0}, identifier={1}", ji.getId(), ei);
    }

    public boolean canBeFinish(JobIdentity ji, ExecutorIdentifier ei, ResourceID resourceID) {
        if (isExecutorExist(ei, resourceID)) {
            log.info("Executor be found, jobId={}, identifier={}", ji.getId(), ei);
            return true;
        }
        String portString = Optional.ofNullable(
                JobConfigurationHolder.getJobConfiguration().getHostProperties().getPort())
                .orElse(DefaultExecutorIdentifier.DEFAULT_PORT + "");
        if (SystemUtils.getLocalIpAddress().equals(ei.getHost()) && Objects.equals(portString, ei.getPort() + "")) {
            return true;
        }
        if (!isOdcHealthy(ei.getHost(), ei.getPort())) {
            log.info("Cannot connect to target odc server, executor can be destroyed,jobId={}, identifier={}",
                    ji.getId(), ei);
            return true;
        }
        return false;
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

    @Override
    protected boolean isExecutorExist(ExecutorIdentifier identifier, ResourceID resourceID) {
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

    private boolean isOdcHealthy(String server, int servPort) {
        String url = String.format("http://%s:%d/api/v1/heartbeat/isHealthy", server, servPort);
        try {
            OdcResult<Boolean> result = HttpClientUtils.request("GET", url, new TypeReference<OdcResult<Boolean>>() {});
            return result.getData();
        } catch (IOException e) {
            log.warn("Check odc server health failed, url={}", url);
            return false;
        }
    }

}
