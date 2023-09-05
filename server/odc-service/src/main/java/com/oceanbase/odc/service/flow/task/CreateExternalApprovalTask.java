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

package com.oceanbase.odc.service.flow.task;

import java.util.Objects;
import java.util.Optional;

import org.flowable.engine.delegate.DelegateExecution;
import org.springframework.beans.factory.annotation.Autowired;

import com.oceanbase.odc.common.util.RetryExecutor;
import com.oceanbase.odc.core.flow.BaseFlowableDelegate;
import com.oceanbase.odc.core.shared.constant.FlowStatus;
import com.oceanbase.odc.metadb.flow.FlowInstanceRepository;
import com.oceanbase.odc.service.flow.FlowableAdaptor;
import com.oceanbase.odc.service.flow.instance.FlowApprovalInstance;
import com.oceanbase.odc.service.flow.model.FlowNodeStatus;
import com.oceanbase.odc.service.flow.task.model.RuntimeTaskConstants;
import com.oceanbase.odc.service.flow.util.FlowTaskUtil;
import com.oceanbase.odc.service.integration.IntegrationService;
import com.oceanbase.odc.service.integration.client.ApprovalClient;
import com.oceanbase.odc.service.integration.model.ApprovalProperties;
import com.oceanbase.odc.service.integration.model.IntegrationConfig;
import com.oceanbase.odc.service.integration.model.TemplateVariables;

import lombok.extern.slf4j.Slf4j;

/**
 * @author gaoda.xy
 * @date 2023/9/1 14:00
 */
@Slf4j
public class CreateExternalApprovalTask extends BaseFlowableDelegate {

    @Autowired
    private FlowableAdaptor flowableAdaptor;
    @Autowired
    private IntegrationService integrationService;
    @Autowired
    private ApprovalClient approvalClient;
    @Autowired
    private FlowInstanceRepository flowInstanceRepository;
    private final RetryExecutor retryExecutor = RetryExecutor.builder().retryIntervalMillis(1000).retryTimes(3).build();

    @Override
    protected void run(DelegateExecution execution) throws Exception {
        FlowApprovalInstance flowApprovalInstance;
        try {
            flowApprovalInstance = getFlowApprovalInstance(execution);
        } catch (Exception e) {
            log.warn("Get flow approval instance failed, activityId={}, processDefinitionId={}",
                    execution.getCurrentActivityId(), execution.getProcessDefinitionId(), e);
            return;
        }
        Long externalApprovalId = flowApprovalInstance.getExternalApprovalId();
        String expr = RuntimeTaskConstants.SUCCESS_CREATE_EXT_INS + "_" + flowApprovalInstance.getId();
        if (Objects.nonNull(externalApprovalId)) {
            try {
                IntegrationConfig config = integrationService.detailWithoutPermissionCheck(externalApprovalId);
                ApprovalProperties properties = ApprovalProperties.from(config);
                TemplateVariables variables = FlowTaskUtil.getTemplateVariables(execution.getVariables());
                String externalFlowInstanceId = approvalClient.start(properties, variables);
                flowApprovalInstance.setExternalFlowInstanceId(externalFlowInstanceId);
                flowApprovalInstance.update();
                execution.setVariable(expr, true);
            } catch (Exception e) {
                log.warn("Create external approval instance failed, the flow instance is coming to an end, "
                        + "flowApprovalInstanceId={}, externalApprovalId={}",
                        flowApprovalInstance.getId(), externalApprovalId, e);
                flowApprovalInstance.setStatus(FlowNodeStatus.FAILED);
                flowApprovalInstance.setComment(e.getLocalizedMessage());
                flowApprovalInstance.update();
                flowInstanceRepository.updateStatusById(flowApprovalInstance.getFlowInstanceId(),
                        FlowStatus.EXECUTION_FAILED);
                execution.setVariable(expr, false);
            }
        }
    }

    private FlowApprovalInstance getFlowApprovalInstance(DelegateExecution execution) {
        String activityId = execution.getCurrentActivityId();
        String processDefinitionId = execution.getProcessDefinitionId();
        Optional<Optional<Long>> flowInstanceIdOpt = retryExecutor.run(
                () -> flowableAdaptor.getFlowInstanceIdByProcessDefinitionId(processDefinitionId), Optional::isPresent);
        if (!flowInstanceIdOpt.isPresent() || !flowInstanceIdOpt.get().isPresent()) {
            log.warn("Flow instance id does not exist, activityId={}, processDefinitionId={}", activityId,
                    processDefinitionId);
            throw new IllegalStateException(
                    "Can not find flow instance id by process definition id " + processDefinitionId);
        }
        Long flowInstanceId = flowInstanceIdOpt.get().get();
        Optional<FlowApprovalInstance> instanceOpt =
                flowableAdaptor.getApprovalInstanceByActivityId(activityId, flowInstanceId);
        if (!instanceOpt.isPresent()) {
            log.warn("Flow approval instance does not exist, activityId={}, flowInstanceId={}", activityId,
                    flowInstanceId);
            throw new IllegalStateException("Can not find instance by activityId " + activityId);
        }
        return instanceOpt.get();
    }

}
