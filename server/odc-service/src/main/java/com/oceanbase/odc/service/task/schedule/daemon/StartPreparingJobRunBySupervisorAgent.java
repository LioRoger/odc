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

package com.oceanbase.odc.service.task.schedule.daemon;

import java.text.MessageFormat;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections4.CollectionUtils;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.data.domain.Page;

import com.google.common.collect.Lists;
import com.oceanbase.odc.common.trace.TraceContextHolder;
import com.oceanbase.odc.core.alarm.AlarmEventNames;
import com.oceanbase.odc.core.alarm.AlarmUtils;
import com.oceanbase.odc.metadb.task.JobEntity;
import com.oceanbase.odc.metadb.task.SupervisorEndpointEntity;
import com.oceanbase.odc.metadb.task.SupervisorEndpointRepository;
import com.oceanbase.odc.service.task.caller.JobCallerBuilder;
import com.oceanbase.odc.service.task.caller.JobContext;
import com.oceanbase.odc.service.task.caller.JobEnvironmentFactory;
import com.oceanbase.odc.service.task.caller.ProcessJobCaller;
import com.oceanbase.odc.service.task.config.JobConfiguration;
import com.oceanbase.odc.service.task.config.JobConfigurationHolder;
import com.oceanbase.odc.service.task.config.TaskFrameworkProperties;
import com.oceanbase.odc.service.task.enums.JobStatus;
import com.oceanbase.odc.service.task.enums.TaskRunMode;
import com.oceanbase.odc.service.task.exception.JobException;
import com.oceanbase.odc.service.task.exception.TaskRuntimeException;
import com.oceanbase.odc.service.task.schedule.DefaultJobContextBuilder;
import com.oceanbase.odc.service.task.schedule.SingleJobProperties;
import com.oceanbase.odc.service.task.service.TaskFrameworkService;
import com.oceanbase.odc.service.task.supervisor.SupervisorEndpointState;
import com.oceanbase.odc.service.task.supervisor.endpoint.ExecutorEndpoint;
import com.oceanbase.odc.service.task.supervisor.endpoint.SupervisorEndpoint;
import com.oceanbase.odc.service.task.util.JobDateUtils;
import com.oceanbase.odc.service.task.util.TaskSupervisorUtil;

import cn.hutool.core.util.StrUtil;
import lombok.extern.slf4j.Slf4j;

/**
 * prepare job run by supervisor agent
 * @author longpeng.zlp
 * @date 2024/11/29 14:08
 */
@Slf4j
@DisallowConcurrentExecution
public class StartPreparingJobRunBySupervisorAgent implements Job {

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        JobConfiguration configuration = JobConfigurationHolder.getJobConfiguration();

        if (!configuration.getTaskFrameworkEnabledProperties().isEnabled()) {
            configuration.getTaskFrameworkDisabledHandler().handleJobToFailed();
            return;
        }

        TaskFrameworkProperties taskFrameworkProperties = configuration.getTaskFrameworkProperties();
        // scan preparing job
        TaskFrameworkService taskFrameworkService = configuration.getTaskFrameworkService();
        Page<JobEntity> jobs = taskFrameworkService.find(
            Lists.newArrayList(JobStatus.PREPARING, JobStatus.RETRYING), 0,
            taskFrameworkProperties.getSingleFetchPreparingJobRows());

        for (JobEntity jobEntity : jobs) {
            if (!configuration.getStartJobRateLimiter().tryAcquire()) {
                break;
            }
            try {
                if (checkJobIsExpired(jobEntity)) {
                    taskFrameworkService.updateStatusDescriptionByIdOldStatus(jobEntity.getId(),
                        jobEntity.getStatus(), JobStatus.CANCELED, "Job expired and failed.");
                } else {
                    SupervisorEndpoint supervisorEndpoint = chooseSupervisorEndpoint(configuration.getSupervisorEndpointRepository());
                    // no resource found
                    if (null == supervisorEndpoint) {
                        break;
                    }
                    startJob(supervisorEndpoint, configuration, jobEntity);
                }
            } catch (Throwable e) {
                log.warn("Start job failed, jobId={}.", jobEntity.getId(), e);
            }
        }

    }

    private void startJob(SupervisorEndpoint supervisorEndpoint, JobConfiguration configuration, JobEntity jobEntity) {
        if (jobEntity.getStatus() == JobStatus.PREPARING || jobEntity.getStatus() == JobStatus.RETRYING) {
            // todo user id should be not null when submit job
            if (jobEntity.getCreatorId() != null) {
                TraceContextHolder.setUserId(jobEntity.getCreatorId());
            }

            log.info("Prepare start job, jobId={}, currentStatus={}.",
                jobEntity.getId(), jobEntity.getStatus());
            JobContext jobContext =
                new DefaultJobContextBuilder().build(jobEntity);
            ProcessJobCaller jobCaller = JobCallerBuilder.buildProcessCaller(jobContext,
                new JobEnvironmentFactory().build(jobContext, TaskRunMode.PROCESS));
            try {
                ExecutorEndpoint executorEndpoint = configuration.getTaskSupervisorJobCaller().startTask(supervisorEndpoint, jobContext, jobCaller.getProcessConfig());
                log.info("start job success with endpoint={}", executorEndpoint);
            } catch (JobException e) {
                Map<String, String> eventMessage = AlarmUtils.createAlarmMapBuilder()
                    .item(AlarmUtils.ORGANIZATION_NAME, Optional.ofNullable(jobEntity.getOrganizationId()).map(
                        Object::toString).orElse(StrUtil.EMPTY))
                    .item(AlarmUtils.TASK_JOB_ID_NAME, String.valueOf(jobEntity.getId()))
                    .item(AlarmUtils.MESSAGE_NAME,
                        MessageFormat.format("Start job failed, jobId={0}, message={1}",
                            jobEntity.getId(),
                            e.getMessage()))
                    .build();
                AlarmUtils.alarm(AlarmEventNames.TASK_START_FAILED, eventMessage);
                // rollback load
                TaskSupervisorUtil.releaseLoad(configuration.getSupervisorEndpointRepository(), supervisorEndpoint);
                throw new TaskRuntimeException(e);
            }
        } else {
            log.warn("Job {} current status is {} but not preparing or retrying, start explain is aborted.",
                jobEntity.getId(), jobEntity.getStatus());
        }

    }

    private boolean checkJobIsExpired(JobEntity jobEntity) {
        SingleJobProperties jobProperties = SingleJobProperties.fromJobProperties(jobEntity.getJobProperties());
        if (jobProperties == null || jobProperties.getJobExpiredIfNotRunningAfterSeconds() == null) {
            return false;
        }

        long baseTimeMills = jobEntity.getCreateTime().getTime();
        return JobDateUtils.getCurrentDate().getTime() - baseTimeMills > TimeUnit.MILLISECONDS.convert(
            jobProperties.getJobExpiredIfNotRunningAfterSeconds(), TimeUnit.SECONDS);
    }

    /**
     * this logic will wrap to a interface
     * @return
     */
    protected SupervisorEndpoint chooseSupervisorEndpoint(SupervisorEndpointRepository supervisorEndpointRepository) {
        List<SupervisorEndpointEntity> supervisorEndpointEntities = TaskSupervisorUtil.find(supervisorEndpointRepository, Collections.singletonList(
            SupervisorEndpointState.AVAILABLE.name()), 0, 100).getContent();
        // no available found
        if (CollectionUtils.isEmpty(supervisorEndpointEntities)) {
            // no endpoint found, that's not good
            log.warn("not supervisor end point found");
            return null;
        }
        // use load smaller
        supervisorEndpointEntities.sort((s1, s2) -> Integer.compare(s1.getLoad(), s2.getLoad()));
        SupervisorEndpointEntity tmp = supervisorEndpointEntities.get(0);
        SupervisorEndpoint ret = new SupervisorEndpoint(tmp.getHost(), tmp.getPort());
        // each task means one load
        supervisorEndpointRepository.addLoadByHostAndPort(tmp.getHost(), tmp.getPort(),  1);
        return ret;
    }
}
