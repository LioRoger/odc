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
package com.oceanbase.odc.server.web.controller.v2;

import java.util.Date;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.web.PageableDefault;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.oceanbase.odc.core.shared.exception.UnsupportedException;
import com.oceanbase.odc.service.common.response.ListResponse;
import com.oceanbase.odc.service.common.response.PaginatedResponse;
import com.oceanbase.odc.service.common.response.Responses;
import com.oceanbase.odc.service.common.response.SuccessResponse;
import com.oceanbase.odc.service.dlm.DlmLimiterService;
import com.oceanbase.odc.service.dlm.model.RateLimitConfiguration;
import com.oceanbase.odc.service.schedule.ScheduleService;
import com.oceanbase.odc.service.schedule.flowtask.OperationType;
import com.oceanbase.odc.service.schedule.model.QueryScheduleParams;
import com.oceanbase.odc.service.schedule.model.ScheduleChangeLog;
import com.oceanbase.odc.service.schedule.model.ScheduleChangeReq;
import com.oceanbase.odc.service.schedule.model.ScheduleDetailResp;
import com.oceanbase.odc.service.schedule.model.ScheduleListResp;
import com.oceanbase.odc.service.schedule.model.ScheduleStatus;
import com.oceanbase.odc.service.schedule.model.ScheduleTaskDetailResp;
import com.oceanbase.odc.service.schedule.model.ScheduleTaskListResp;
import com.oceanbase.odc.service.schedule.model.ScheduleType;
import com.oceanbase.odc.service.task.model.OdcTaskLogLevel;

import io.swagger.annotations.ApiOperation;

/**
 * @Author：tinker
 * @Date: 2022/11/22 14:27
 * @Descripition:
 */

@RestController
@RequestMapping("/api/v2/schedule")
public class ScheduleController {
    @Autowired
    private ScheduleService scheduleService;

    @Autowired
    private DlmLimiterService dlmLimiterService;


    // change log

    @RequestMapping("/schedules/{id:[\\d]+}/changes")
    public ListResponse<ScheduleChangeLog> listChangeLog(@PathVariable Long id) {
        List<ScheduleChangeLog> scheduleChangeLogs = scheduleService.listScheduleChangeLog(id);
        return Responses.list(scheduleChangeLogs);
    }

    @RequestMapping("/schedules/{id:[\\d]+}/changes/{scheduleChangeLogId:[\\d]+}")
    public SuccessResponse<ScheduleChangeLog> getChangeLog(@PathVariable Long id,
            @PathVariable Long scheduleChangeLogId) {
        return Responses.single(scheduleService.getChangeLog(id, scheduleChangeLogId));
    }

    // schedule task

    @RequestMapping(value = "/schedules/{scheduleId:[\\d]+}/tasks/{taskId:[\\d]+}/executions/latest/terminate",
            method = RequestMethod.POST)
    public void terminateTask(@PathVariable Long scheduleId, @PathVariable Long taskId) {
        throw new UnsupportedException();
    }

    @RequestMapping(value = "/schedules/{scheduleId:[\\d]+}/tasks/{taskId:[\\d]+}/executions/latest/stop",
            method = RequestMethod.POST)
    public SuccessResponse<Boolean> stopTask(@PathVariable Long scheduleId, @PathVariable Long taskId) {
        scheduleService.stopTask(scheduleId, taskId);
        return Responses.ok(Boolean.TRUE);
    }

    @ApiOperation(value = "StartTask", notes = "启动任务")
    @RequestMapping(value = "/schedules/{scheduleId:[\\d]+}/tasks/{taskId:[\\d]+}/executions/latest/start",
            method = RequestMethod.POST)
    public SuccessResponse<Boolean> startTask(@PathVariable Long scheduleId, @PathVariable Long taskId) {
        scheduleService.startTask(scheduleId, taskId);
        return Responses.ok(Boolean.TRUE);
    }

    @RequestMapping(value = "/schedules/{scheduleId:[\\d]+}/tasks/{taskId:[\\d]+}/executions/latest/rollback",
            method = RequestMethod.POST)
    public SuccessResponse<Boolean> rollbackTask(@PathVariable Long scheduleId, @PathVariable Long taskId) {
        return Responses.ok(Boolean.TRUE);
    }

    @RequestMapping(value = "/schedules/{scheduleId:[\\d]+}/tasks/{taskId:[\\d]+}/executions/latest/log",
            method = RequestMethod.GET)
    public SuccessResponse<String> getTaskLog(@PathVariable Long scheduleId, @PathVariable Long taskId,
            @RequestParam OdcTaskLogLevel logType) {
        return Responses.single(scheduleService.getLog(scheduleId, taskId, logType));
    }


    @RequestMapping(value = "/schedules/{scheduleId:[\\d]+}/tasks/{taskId:[\\d]+}/", method = RequestMethod.GET)
    public SuccessResponse<ScheduleTaskDetailResp> detailScheduleTask(@PathVariable Long scheduleId,
            @PathVariable Long taskId) {
        return Responses.single(scheduleService.detailScheduleTask(scheduleId, taskId));
    }

    @RequestMapping(value = "/schedules/{scheduleId:[\\d]+}/tasks", method = RequestMethod.GET)
    public PaginatedResponse<ScheduleTaskListResp> listTask(
            @PageableDefault(size = Integer.MAX_VALUE, sort = {"id"}, direction = Direction.DESC) Pageable pageable,
            @PathVariable Long scheduleId) {
        return Responses.paginated(scheduleService.listScheduleTask(pageable, scheduleId));
    }

    // schedule

    @RequestMapping(value = "/schedules/{id:[\\d]+}/terminate", method = RequestMethod.POST)
    public void terminateSchedule(@PathVariable("id") Long id) {
        scheduleService.terminate(id);
    }

    @RequestMapping(value = "/schedules/{id:[\\d]+}/pause", method = RequestMethod.POST)
    public void pauseSchedule(@PathVariable Long id) {
        scheduleService.pause(id);
    }

    @RequestMapping(value = "/schedules/{id:[\\d]+}/resume", method = RequestMethod.POST)
    public void resumeSchedule(@PathVariable Long id) {
        scheduleService.resume(id);
    }

    @RequestMapping(value = "/schedules/{id:[\\d]+}", method = RequestMethod.PUT)
    public void updateSchedule(@PathVariable Long id, @RequestBody ScheduleChangeReq req) {
        scheduleService.changeSchedule(req);
    }

    @RequestMapping(value = "/schedules", method = RequestMethod.POST)
    public void createSchedule(@RequestBody ScheduleChangeReq req) {
        req.setOperationType(OperationType.CREATE);
        scheduleService.changeSchedule(req);
    }


    @RequestMapping("/schedules")
    public PaginatedResponse<ScheduleListResp> list(
            @PageableDefault(size = Integer.MAX_VALUE, sort = {"id"}, direction = Direction.DESC) Pageable pageable,
            @RequestParam(required = false, name = "connectionId") List<Long> connectionIds,
            @RequestParam(required = false, name = "id") Long id,
            @RequestParam(required = false, name = "status") List<ScheduleStatus> status,
            @RequestParam(required = false, name = "type") ScheduleType type,
            @RequestParam(required = false, name = "startTime") Date startTime,
            @RequestParam(required = false, name = "endTime") Date endTime,
            @RequestParam(required = false, name = "creator") String creator,
            @RequestParam(required = false, name = "projectId") Long projectId) {

        QueryScheduleParams req = QueryScheduleParams.builder()
                .id(id)
                .connectionIds(connectionIds)
                .statuses(status)
                .type(type)
                .startTime(startTime)
                .endTime(endTime)
                .creator(creator)
                .projectId(projectId)
                .build();
        return Responses.paginated(scheduleService.getScheduleListResp(pageable, req));

    }

    @RequestMapping(value = "/schedules/{id:[\\d]+}", method = RequestMethod.GET)
    public SuccessResponse<ScheduleDetailResp> detailSchedule(@PathVariable Long id) {
        return Responses.single(scheduleService.detailSchedule(id));
    }


    @RequestMapping(value = "/{id:[\\d]+}/jobs/async/batchGetDownloadUrl", method = RequestMethod.POST)
    public ListResponse<String> getDownloadUrl(@PathVariable Long id, @RequestBody List<String> objectId) {
        return Responses.list(scheduleService.getAsyncDownloadUrl(id, objectId));
    }

    @RequestMapping(value = "/schedules/{id:[\\d]+}/dlmRateLimitConfiguration", method = RequestMethod.PUT)
    public SuccessResponse<RateLimitConfiguration> updateLimiterConfig(@PathVariable Long id,
            @RequestBody RateLimitConfiguration limiterConfig) {
        return Responses.single(dlmLimiterService.updateByOrderId(id, limiterConfig));
    }

}
