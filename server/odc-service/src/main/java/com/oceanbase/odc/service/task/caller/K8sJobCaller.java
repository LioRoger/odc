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

import java.util.Collections;
import java.util.Optional;

import com.oceanbase.odc.service.task.exception.JobException;
import com.oceanbase.odc.service.task.resource.Resource;
import com.oceanbase.odc.service.task.resource.ResourceID;
import com.oceanbase.odc.service.task.resource.ResourceType;
import com.oceanbase.odc.service.task.resource.k8s.K8SResourceManager;
import com.oceanbase.odc.service.task.resource.k8s.K8sResource;
import com.oceanbase.odc.service.task.resource.k8s.PodConfig;
import com.oceanbase.odc.service.task.schedule.JobIdentity;
import com.oceanbase.odc.service.task.util.JobUtils;

import lombok.extern.slf4j.Slf4j;

/**
 * @author yaobin
 * @date 2023-11-15
 * @since 4.2.4
 */
@Slf4j
public class K8sJobCaller extends BaseJobCaller {

    /**
     * base job config
     */
    private final PodConfig defaultPodConfig;
    private final K8SResourceManager resourceManager;

    public K8sJobCaller(PodConfig podConfig, K8SResourceManager resourceManager) {
        this.defaultPodConfig = podConfig;
        this.resourceManager = resourceManager;
    }

    @Override
    public ExecutorIdentifier doStart(JobContext context) throws JobException {
        String jobName = JobUtils.generateExecutorName(context.getJobIdentity());
        // TODO(lx): config it
        Resource resource = resourceManager.create(ResourceType.REMOTE_K8S, jobName, defaultPodConfig);
        String arn = resource.id().getName();

        return DefaultExecutorIdentifier.builder().namespace(defaultPodConfig.getNamespace())
                .executorName(arn).build();
    }

    @Override
    public void doStop(JobIdentity ji) throws JobException {}

    @Override
    protected void doDestroy(JobIdentity ji, ExecutorIdentifier ei, ResourceID resourceID) throws JobException {
        // update job destroyed
        updateExecutorDestroyed(ji);
        resourceManager.destroy(resourceID);
    }

    @Override
    protected boolean canBeDestroy(JobIdentity ji, ExecutorIdentifier ei, ResourceID resourceID) {
        return resourceManager.canBeDestroyed(resourceID);
    }

    @Override
    protected boolean isExecutorExist(ExecutorIdentifier identifier) throws JobException {
        ResourceID resourceID =
                new ResourceID(identifier.getNamespace(), identifier.getExecutorName(), Collections.emptyMap());
        Optional<K8sResource> executorOptional = resourceManager.query(resourceID);
        return executorOptional.isPresent() &&
                PodStatus.of(executorOptional.get().getResourceStatus()) != PodStatus.TERMINATING;
    }
}
