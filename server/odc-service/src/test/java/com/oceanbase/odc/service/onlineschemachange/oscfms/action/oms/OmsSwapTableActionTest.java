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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import com.oceanbase.odc.core.session.ConnectionSession;
import com.oceanbase.odc.core.session.ConnectionSessionConstants;
import com.oceanbase.odc.core.shared.constant.DialectType;
import com.oceanbase.odc.core.shared.constant.TaskStatus;
import com.oceanbase.odc.core.sql.execute.SyncJdbcExecutor;
import com.oceanbase.odc.service.onlineschemachange.configuration.OnlineSchemaChangeProperties;
import com.oceanbase.odc.service.onlineschemachange.configuration.OnlineSchemaChangeProperties.OmsProperties;
import com.oceanbase.odc.service.onlineschemachange.model.OnlineSchemaChangeParameters;
import com.oceanbase.odc.service.onlineschemachange.oms.enums.OmsProjectStatusEnum;
import com.oceanbase.odc.service.onlineschemachange.oms.enums.OmsStepName;
import com.oceanbase.odc.service.onlineschemachange.oms.enums.OmsStepStatus;
import com.oceanbase.odc.service.onlineschemachange.oms.openapi.OmsProjectOpenApiService;
import com.oceanbase.odc.service.onlineschemachange.oms.response.OmsProjectProgressResponse;
import com.oceanbase.odc.service.onlineschemachange.oms.response.OmsProjectStepVO;
import com.oceanbase.odc.service.onlineschemachange.oscfms.OscActionContext;
import com.oceanbase.odc.service.onlineschemachange.oscfms.OscActionResult;
import com.oceanbase.odc.service.onlineschemachange.oscfms.OscTestUtil;
import com.oceanbase.odc.service.onlineschemachange.oscfms.action.oms.ProjectStepResultChecker.ProjectStepResult;
import com.oceanbase.odc.service.onlineschemachange.oscfms.state.OscStates;
import com.oceanbase.odc.service.onlineschemachange.rename.DefaultRenameTableInvoker;
import com.oceanbase.odc.service.onlineschemachange.rename.RenameTableHandler;
import com.oceanbase.odc.service.session.DBSessionManageFacade;

/**
 * @author longpeng.zlp
 * @date 2024/7/30 16:34
 * @since 4.3.1
 */
public class OmsSwapTableActionTest {
    private DBSessionManageFacade dbSessionManageFacade;
    private OmsProjectOpenApiService omsProjectOpenApiService;
    private OnlineSchemaChangeProperties onlineSchemaChangeProperties;

    @Before
    public void init() {
        dbSessionManageFacade = Mockito.mock(DBSessionManageFacade.class);
        omsProjectOpenApiService = Mockito.mock(OmsProjectOpenApiService.class);
        onlineSchemaChangeProperties = new OnlineSchemaChangeProperties();
        onlineSchemaChangeProperties.setEnableFullVerify(false);
        OmsProperties omsProperties = new OmsProperties();
        omsProperties.setUrl("127.0.0.1:8089");
        omsProperties.setRegion("default");
        omsProperties.setAuthorization("auth");
        onlineSchemaChangeProperties.setOms(omsProperties);
    }

    @Test
    public void testCheckOmsProjectNotReady() throws Exception {
        OscActionContext context = OscTestUtil.createOcsActionContext(DialectType.OB_MYSQL,
                OscStates.SWAP_TABLE.getState(), TaskStatus.RUNNING);
        Mockito.when(omsProjectOpenApiService.describeProjectProgress(ArgumentMatchers.any()))
                .thenReturn(getOMSResponse(OmsProjectStatusEnum.RUNNING));
        Mockito.when(omsProjectOpenApiService.describeProjectSteps(ArgumentMatchers.any()))
                .thenReturn(getProjectSteps());
        OmsSwapTableAction swapTableAction = new OmsSwapTableAction(dbSessionManageFacade, omsProjectOpenApiService,
                onlineSchemaChangeProperties);
        OscActionResult result = swapTableAction.execute(context);
        Assert.assertEquals(result.getNextState(), OscStates.SWAP_TABLE.getState());
    }

    @Test
    public void testDefaultRenameTableInvoker() {
        ConnectionSession connectionSession = Mockito.mock(ConnectionSession.class);
        Mockito.when(connectionSession.getDialectType()).thenReturn(DialectType.OB_MYSQL);
        Mockito.when(connectionSession.getAttribute(ConnectionSessionConstants.OB_VERSION)).thenReturn("3.4.0");
        Mockito.when(connectionSession.getSyncJdbcExecutor(
                ArgumentMatchers.anyString())).thenReturn(Mockito.mock(SyncJdbcExecutor.class));
        MockRenameTableHandler mockRenameTableHandler = new MockRenameTableHandler();
        DefaultRenameTableInvoker renameTableInvoker =
                new DefaultRenameTableInvoker(connectionSession, dbSessionManageFacade, mockRenameTableHandler,
                        () -> true);
        OnlineSchemaChangeParameters parameters = OscTestUtil.createOscParameters();
        parameters.setSwapTableNameRetryTimes(1);
        renameTableInvoker.invoke(
                OscTestUtil.createTaskParameters(DialectType.OB_MYSQL, OscStates.SWAP_TABLE.getState()),
                parameters);
        Assert.assertEquals(mockRenameTableHandler.renamePair.size(), 1);
        Assert.assertEquals(mockRenameTableHandler.renamePair.get(0), "ghost_test_table");
    }

    @Test
    public void testSwapTableReady() {
        // MockStatic instance should closed after use
        try (MockedStatic<OmsRequestUtil> requestUtil = Mockito.mockStatic(OmsRequestUtil.class);) {
            ProjectStepResult projectStepResult = new ProjectStepResult();
            // valid checkpoint
            long checkpoint = System.currentTimeMillis() / 1000 + 10;
            projectStepResult.setIncrementCheckpoint(checkpoint);
            requestUtil.when(() -> OmsRequestUtil.buildProjectStepResult(ArgumentMatchers.any(),
                    ArgumentMatchers.any(), ArgumentMatchers.anyString(), ArgumentMatchers.anyString(),
                    ArgumentMatchers.anyString(),
                    ArgumentMatchers.any())).thenReturn(projectStepResult);
            requestUtil.when(() -> OmsRequestUtil.isOmsTaskReady(projectStepResult)).thenReturn(true);
            OmsSwapTableAction swapTableAction = new OmsSwapTableAction(dbSessionManageFacade, omsProjectOpenApiService,
                    onlineSchemaChangeProperties);
            Assert.assertTrue(
                    swapTableAction.isIncrementDataAppliedDone(omsProjectOpenApiService, onlineSchemaChangeProperties,
                            "uid", "projectID", "db", Collections.emptyMap(), 1000));
        }
    }

    @Test
    public void testSwapTableNotReady() {
        try (MockedStatic<OmsRequestUtil> requestUtil = Mockito.mockStatic(OmsRequestUtil.class);) {
            ProjectStepResult projectStepResult = new ProjectStepResult();
            // valid checkpoint
            long checkpoint = System.currentTimeMillis() / 1000 - 10;
            projectStepResult.setIncrementCheckpoint(checkpoint);
            requestUtil.when(() -> OmsRequestUtil.buildProjectStepResult(ArgumentMatchers.any(),
                    ArgumentMatchers.any(), ArgumentMatchers.anyString(), ArgumentMatchers.anyString(),
                    ArgumentMatchers.anyString(),
                    ArgumentMatchers.any())).thenReturn(projectStepResult);
            requestUtil.when(() -> OmsRequestUtil.isOmsTaskReady(projectStepResult)).thenReturn(true);
            OmsSwapTableAction swapTableAction = new OmsSwapTableAction(dbSessionManageFacade, omsProjectOpenApiService,
                    onlineSchemaChangeProperties);
            long currentTimeMS = System.currentTimeMillis();
            Assert.assertFalse(
                    swapTableAction.isIncrementDataAppliedDone(omsProjectOpenApiService, onlineSchemaChangeProperties,
                            "uid", "projectID", "db", Collections.emptyMap(), 3000));
            long endTimeMS = System.currentTimeMillis();
            // test retry
            Assert.assertTrue(endTimeMS - currentTimeMS > 2000);
        }
    }

    private OmsProjectProgressResponse getOMSResponse(OmsProjectStatusEnum statusEnum) {
        OmsProjectProgressResponse omsProjectResponse = new OmsProjectProgressResponse();
        omsProjectResponse.setStatus(statusEnum);
        return omsProjectResponse;
    }

    private List<OmsProjectStepVO> getProjectSteps() {
        OmsProjectStepVO fullStep = new OmsProjectStepVO();
        fullStep.setName(OmsStepName.FULL_TRANSFER);
        fullStep.setProgress(100);
        fullStep.setStatus(OmsStepStatus.FINISHED);
        OmsProjectStepVO incrStep = new OmsProjectStepVO();
        incrStep.setName(OmsStepName.INCR_TRANSFER);
        incrStep.setProgress(100);
        incrStep.setStatus(OmsStepStatus.FINISHED);
        OmsProjectStepVO preCheck = new OmsProjectStepVO();
        preCheck.setName(OmsStepName.TRANSFER_PRECHECK);
        preCheck.setProgress(100);
        preCheck.setStatus(OmsStepStatus.FINISHED);
        OmsProjectStepVO incrLogPull = new OmsProjectStepVO();
        incrLogPull.setName(OmsStepName.TRANSFER_INCR_LOG_PULL);
        incrLogPull.setProgress(100);
        incrLogPull.setStatus(OmsStepStatus.FINISHED);
        return Arrays.asList(incrLogPull, preCheck, fullStep, incrStep);
    }

    private static final class MockRenameTableHandler implements RenameTableHandler {
        List<String> renamePair = new ArrayList<>();

        @Override
        public void rename(String schema, String fromName, String toName) {
            renamePair.add(toName);
        }

        @Override
        public void rename(String schema, String originName, String oldName, String newName) {
            renamePair.add(newName);
        }
    }

}
