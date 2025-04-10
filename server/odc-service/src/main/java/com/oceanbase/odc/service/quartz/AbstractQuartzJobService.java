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
package com.oceanbase.odc.service.quartz;

import java.text.ParseException;
import java.util.HashMap;
import java.util.List;

import org.quartz.CronScheduleBuilder;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.UnableToInterruptJobException;

import com.oceanbase.odc.core.shared.exception.UnexpectedException;
import com.oceanbase.odc.core.shared.exception.UnsupportedException;
import com.oceanbase.odc.service.quartz.model.MisfireStrategy;
import com.oceanbase.odc.service.quartz.util.QuartzCronExpressionUtils;
import com.oceanbase.odc.service.schedule.model.ChangeQuartJobParam;
import com.oceanbase.odc.service.schedule.model.CreateQuartzJobParam;
import com.oceanbase.odc.service.schedule.model.QuartzKeyGenerator;
import com.oceanbase.odc.service.schedule.model.TriggerConfig;

/**
 * @author jingtian
 * @date 2024/12/25
 */
public abstract class AbstractQuartzJobService {
    public void changeJob(ChangeQuartJobParam req) {

        JobKey jobKey = QuartzKeyGenerator.generateJobKey(req.getJobName(), req.getJobGroup());
        try {
            switch (req.getOperationType()) {
                case CREATE: {
                    CreateQuartzJobParam createQuartzJobReq = new CreateQuartzJobParam();
                    createQuartzJobReq.setJobKey(jobKey);
                    createQuartzJobReq.setAllowConcurrent(req.getAllowConcurrent());
                    createQuartzJobReq.setMisfireStrategy(req.getMisfireStrategy());
                    createQuartzJobReq.setTriggerConfig(req.getTriggerConfig());
                    createQuartzJobReq.setJobClass(req.getJobClass());
                    createJob(createQuartzJobReq);
                    break;
                }
                case UPDATE: {
                    deleteJob(jobKey);
                    CreateQuartzJobParam createQuartzJobReq = new CreateQuartzJobParam();
                    createQuartzJobReq.setJobKey(jobKey);
                    createQuartzJobReq.setAllowConcurrent(req.getAllowConcurrent());
                    createQuartzJobReq.setMisfireStrategy(req.getMisfireStrategy());
                    createQuartzJobReq.setTriggerConfig(req.getTriggerConfig());
                    createQuartzJobReq.setJobClass(req.getJobClass());
                    createJob(createQuartzJobReq);
                    break;
                }
                case RESUME: {
                    resumeJob(jobKey);
                    break;
                }
                case PAUSE: {
                    pauseJob(jobKey);
                    break;
                }
                case TERMINATE:
                case DELETE: {
                    deleteJob(jobKey);
                    break;
                }
                default:
                    throw new UnsupportedException();
            }
        } catch (Exception e) {
            throw new UnexpectedException("");
        }
    }

    public void createJob(CreateQuartzJobParam req) throws SchedulerException {
        createJob(req, null);
    }

    // TODO how can we recognize multi trigger for job. maybe we can use jobName as trigger group.
    public void createJob(CreateQuartzJobParam req, JobDataMap triggerDataMap) throws SchedulerException {

        Class<? extends Job> jobClass = req.getJobClass();

        if (req.getTriggerConfig() != null) {
            JobDataMap triData = triggerDataMap == null ? new JobDataMap(new HashMap<>(1)) : triggerDataMap;
            TriggerKey triggerKey =
                    QuartzKeyGenerator.generateTriggerKey(req.getJobKey().getName(), req.getJobKey().getGroup());
            Trigger trigger = buildTrigger(triggerKey, req.getTriggerConfig(), req.getMisfireStrategy(), triData);
            JobDetail jobDetail = JobBuilder.newJob(jobClass).withIdentity(req.getJobKey())
                    .usingJobData(req.getJobDataMap()).build();
            getScheduler().scheduleJob(jobDetail, trigger);
        } else {
            JobDetail jobDetail = JobBuilder.newJob(jobClass).withIdentity(req.getJobKey())
                    .usingJobData(req.getJobDataMap()).storeDurably(true).build();
            getScheduler().addJob(jobDetail, false);
        }
    }

    private Trigger buildTrigger(TriggerKey key, TriggerConfig config,
            MisfireStrategy misfireStrategy, JobDataMap triggerDataMap) throws SchedulerException {
        switch (config.getTriggerStrategy()) {
            case START_NOW:
                return TriggerBuilder
                        .newTrigger().withIdentity(key).withSchedule(SimpleScheduleBuilder
                                .simpleSchedule().withRepeatCount(0).withMisfireHandlingInstructionFireNow())
                        .startNow().usingJobData(triggerDataMap).build();
            case START_AT:
                return TriggerBuilder.newTrigger().withIdentity(key)
                        .withSchedule(SimpleScheduleBuilder.simpleSchedule().withRepeatCount(0)
                                .withMisfireHandlingInstructionFireNow())
                        .startAt(config.getStartAt()).usingJobData(triggerDataMap).build();
            case CRON:
            case DAY:
            case MONTH:
            case WEEK: {

                String cron;

                try {
                    cron = QuartzCronExpressionUtils
                            .adaptCronExpression(config.getCronExpression());
                } catch (ParseException e) {
                    throw new SchedulerException("Invalid cron expression,create quartz job failed.");
                }

                CronScheduleBuilder cronScheduleBuilder = CronScheduleBuilder.cronSchedule(cron);

                switch (misfireStrategy) {
                    case MISFIRE_INSTRUCTION_IGNORE_MISFIRE:
                        cronScheduleBuilder = cronScheduleBuilder.withMisfireHandlingInstructionIgnoreMisfires();
                        break;
                    case MISFIRE_INSTRUCTION_FIRE_ONCE_NOW:
                        cronScheduleBuilder = cronScheduleBuilder.withMisfireHandlingInstructionFireAndProceed();
                        break;
                    default:
                        cronScheduleBuilder = cronScheduleBuilder.withMisfireHandlingInstructionDoNothing();
                        break;
                }

                return TriggerBuilder.newTrigger().withIdentity(key)
                        .withSchedule(cronScheduleBuilder).usingJobData(triggerDataMap).build();

            }
            default:
                throw new UnsupportedException();
        }
    }

    public void pauseJob(JobKey jobKey) throws SchedulerException {
        getScheduler().pauseJob(jobKey);
    }

    public void resumeJob(JobKey jobKey) throws SchedulerException {
        getScheduler().resumeJob(jobKey);
    }

    public void deleteJob(JobKey jobKey) throws SchedulerException {
        getScheduler().deleteJob(jobKey);
    }

    public boolean checkExists(JobKey jobKey) throws SchedulerException {
        return getScheduler().checkExists(jobKey);
    }

    public Trigger getTrigger(TriggerKey key) throws SchedulerException {
        return getScheduler().getTrigger(key);
    }

    public void rescheduleJob(TriggerKey triggerKey, Trigger newTrigger) throws SchedulerException {
        getScheduler().rescheduleJob(triggerKey, newTrigger);
    }

    public void triggerJob(JobKey key) throws SchedulerException {
        getScheduler().triggerJob(key);
    }

    public void interruptJob(JobKey key) throws UnableToInterruptJobException {
        getScheduler().interrupt(key);
    }

    public void triggerJob(JobKey key, JobDataMap triggerDataMap) throws SchedulerException {
        getScheduler().triggerJob(key, triggerDataMap);
    }

    public List<? extends Trigger> getJobTriggers(JobKey jobKey) throws SchedulerException {
        return getScheduler().getTriggersOfJob(jobKey);
    }

    protected abstract Scheduler getScheduler();

}
