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
package com.oceanbase.odc.service.onlineschemachange.oscfms.action.oms;

import java.util.List;

import javax.validation.constraints.NotNull;

import org.apache.commons.collections4.CollectionUtils;

import com.oceanbase.odc.common.json.JsonUtils;
import com.oceanbase.odc.core.session.ConnectionSession;
import com.oceanbase.odc.core.session.ConnectionSessionUtil;
import com.oceanbase.odc.core.shared.PreConditions;
import com.oceanbase.odc.metadb.schedule.ScheduleTaskEntity;
import com.oceanbase.odc.service.connection.model.ConnectionConfig;
import com.oceanbase.odc.service.onlineschemachange.configuration.OnlineSchemaChangeProperties;
import com.oceanbase.odc.service.onlineschemachange.fsm.Action;
import com.oceanbase.odc.service.onlineschemachange.model.OnlineSchemaChangeParameters;
import com.oceanbase.odc.service.onlineschemachange.model.OnlineSchemaChangeScheduleTaskParameters;
import com.oceanbase.odc.service.onlineschemachange.model.OnlineSchemaChangeScheduleTaskResult;
import com.oceanbase.odc.service.onlineschemachange.monitor.DBUserMonitorExecutor;
import com.oceanbase.odc.service.onlineschemachange.oms.openapi.OmsProjectOpenApiService;
import com.oceanbase.odc.service.onlineschemachange.oscfms.OscActionContext;
import com.oceanbase.odc.service.onlineschemachange.oscfms.OscActionResult;
import com.oceanbase.odc.service.onlineschemachange.oscfms.action.oms.ProjectStepResultChecker.ProjectStepResult;
import com.oceanbase.odc.service.onlineschemachange.oscfms.state.OscStates;
import com.oceanbase.odc.service.onlineschemachange.rename.DefaultRenameTableInvoker;
import com.oceanbase.odc.service.onlineschemachange.rename.RenameTableHandlers;
import com.oceanbase.odc.service.session.DBSessionManageFacade;

import lombok.extern.slf4j.Slf4j;

/**
 * swap table action
 * 
 * @author longpeng.zlp
 * @date 2024/7/9 11:38
 * @since 4.3.1
 */
@Slf4j
public class OmsSwapTableAction implements Action<OscActionContext, OscActionResult> {

    private final DBSessionManageFacade dbSessionManageFacade;

    private final OmsProjectOpenApiService projectOpenApiService;

    private final OnlineSchemaChangeProperties onlineSchemaChangeProperties;

    public OmsSwapTableAction(@NotNull DBSessionManageFacade dbSessionManageFacade,
            @NotNull OmsProjectOpenApiService projectOpenApiService,
            @NotNull OnlineSchemaChangeProperties onlineSchemaChangeProperties) {
        this.dbSessionManageFacade = dbSessionManageFacade;
        this.projectOpenApiService = projectOpenApiService;
        this.onlineSchemaChangeProperties = onlineSchemaChangeProperties;
    }

    @Override
    public OscActionResult execute(OscActionContext context) throws Exception {
        if (!checkOMSProject(context)) {
            // OMS state abnormal, keep waiting
            log.info("OSC: swap table waiting for OMS task ready");
            return new OscActionResult(OscStates.SWAP_TABLE.getState(), null, OscStates.SWAP_TABLE.getState());
        }
        // begin swap table
        ScheduleTaskEntity scheduleTask = context.getScheduleTask();
        log.info("Start execute {}, schedule task id {}", getClass().getSimpleName(), scheduleTask.getId());

        OnlineSchemaChangeScheduleTaskParameters taskParameters = context.getTaskParameter();
        PreConditions.notNull(taskParameters, "OnlineSchemaChangeScheduleTaskParameters is null");
        OnlineSchemaChangeParameters parameters = context.getParameter();

        ConnectionConfig config = context.getConnectionProvider().connectionConfig();
        DBUserMonitorExecutor userMonitorExecutor = new DBUserMonitorExecutor(config, parameters.getLockUsers());
        ConnectionSession connectionSession = null;
        try {
            connectionSession = context.getConnectionProvider().createConnectionSession();
            if (enableUserMonitor(parameters.getLockUsers())) {
                userMonitorExecutor.start();
            }
            ConnectionSessionUtil.setCurrentSchema(connectionSession, taskParameters.getDatabaseName());
            DefaultRenameTableInvoker defaultRenameTableInvoker =
                    new DefaultRenameTableInvoker(connectionSession, dbSessionManageFacade,
                            RenameTableHandlers.getForeignKeyHandler(connectionSession));
            defaultRenameTableInvoker.invoke(taskParameters, parameters);
            // rename table success, jump to clean resoruce state
            return new OscActionResult(OscStates.SWAP_TABLE.getState(), null, OscStates.CLEAN_RESOURCE.getState());
        } finally {
            try {
                if (enableUserMonitor(parameters.getLockUsers())) {
                    userMonitorExecutor.stop();
                }
            } finally {
                if (null != connectionSession) {
                    connectionSession.expire();
                }
            }
        }
    }

    protected boolean checkOMSProject(OscActionContext context) {
        OnlineSchemaChangeScheduleTaskParameters taskParameter = context.getTaskParameter();
        // get result
        OnlineSchemaChangeScheduleTaskResult lastResult = JsonUtils.fromJson(context.getScheduleTask().getResultJson(),
                OnlineSchemaChangeScheduleTaskResult.class);
        // get oms step result
        ProjectStepResult projectStepResult =
                OmsRequestUtil.buildProjectStepResult(projectOpenApiService, onlineSchemaChangeProperties,
                        taskParameter.getUid(), taskParameter.getOmsProjectId(), taskParameter.getDatabaseName(),
                        lastResult.getCheckFailedTime());
        return OmsRequestUtil.OMSTaskReady(projectStepResult);
    }

    private boolean enableUserMonitor(List<String> lockUsers) {
        return CollectionUtils.isNotEmpty(lockUsers);
    }
}
