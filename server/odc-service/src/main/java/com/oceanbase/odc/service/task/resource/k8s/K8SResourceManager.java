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
package com.oceanbase.odc.service.task.resource.k8s;

import static com.oceanbase.odc.service.task.constants.JobConstants.ODC_EXECUTOR_CANNOT_BE_DESTROYED;

import java.util.Optional;

import com.oceanbase.odc.service.task.caller.PodStatus;
import com.oceanbase.odc.service.task.exception.JobException;
import com.oceanbase.odc.service.task.resource.Resource;
import com.oceanbase.odc.service.task.resource.ResourceConfig;
import com.oceanbase.odc.service.task.resource.ResourceID;
import com.oceanbase.odc.service.task.resource.ResourceManager;
import com.oceanbase.odc.service.task.resource.ResourceMetaStore;
import com.oceanbase.odc.service.task.resource.ResourceType;
import com.oceanbase.odc.service.task.resource.k8s.client.K8sJobClient;
import com.oceanbase.odc.service.task.resource.k8s.client.K8sJobClientSelector;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * @author longpeng.zlp
 * @date 2024/8/13 10:29
 */
@AllArgsConstructor
@Slf4j
public class K8SResourceManager implements ResourceManager {
    private final K8sJobClientSelector k8sJobClientSelector;
    private final ResourceMetaStore resourceMetaStore;
    private final long podPendingTimeoutSeconds;

    @Override
    public Resource create(ResourceType resourceType, String jobName, ResourceConfig resourceConfig)
            throws JobException {
        K8sJobClient k8sJobClient = selectK8sClient(resourceConfig.resourceGroup());
        PodConfig podConfig = (PodConfig) resourceConfig;
        return k8sJobClient.create(podConfig.getNamespace(), jobName, podConfig.getImage(),
                podConfig.getCommand(), podConfig);
    }

    @Override
    public Optional<K8sResource> query(ResourceID resourceID) throws JobException {
        K8sJobClient k8sJobClient = selectK8sClient(resourceID.getGroup());
        return k8sJobClient.get(resourceID.getGroup(), resourceID.getName());
    }

    @Override
    public String destroy(ResourceID resourceID) throws JobException {
        // check if can be destroyed
        if (!canBeDestroyed(resourceID)) {
            // Pod cannot be deleted when pod pending is not timeout,
            // so throw exception representative delete failed
            throw new JobException(ODC_EXECUTOR_CANNOT_BE_DESTROYED +
                    "Destroy pod failed, resource {}", resourceID);
        }
        K8sJobClient k8sJobClient = selectK8sClient(resourceID.getGroup());
        return k8sJobClient.delete(resourceID.getGroup(), resourceID.getName());
    }

    @Override
    public boolean canBeDestroyed(ResourceID resourceID) {
        Optional<K8sResource> query;
        K8sJobClient k8sJobClient = selectK8sClient(resourceID.getGroup());

        try {
            query = k8sJobClient.get(resourceID.getGroup(), resourceID.getName());
        } catch (JobException e) {
            log.warn("Get k8s pod occur error, resource={}", resourceID, e);
            return false;
        }
        if (query.isPresent()) {
            if (PodStatus.PENDING == PodStatus.of(query.get().getResourceStatus())) {
                if (getResourceCreateTimeInSeconds(resourceID) <= podPendingTimeoutSeconds) {
                    // Pod cannot be deleted when pod pending is not timeout,
                    // so throw exception representative cannot delete
                    log.warn("Cannot destroy pod, pending is not timeout, resourceID={},  podStatus={}",
                            resourceID, query.get().getResourceStatus());
                    return false;
                }
            }
        }
        return isPodIdle();
    }

    // TODO(): impl resource id create time query
    private long getResourceCreateTimeInSeconds(ResourceID resourceID) {
        Resource resource = resourceMetaStore.findResource(resourceID);
        return (System.currentTimeMillis() - resource.createDate().getTime()) / 1000;
    }

    /**
     * if pod is idle, currently always return true if pod is reusable, task counter should impl,
     * rewrite this method
     * 
     * @return true if there is no task running on it
     */
    private boolean isPodIdle() {
        return true;
    }

    private K8sJobClient selectK8sClient(String resourceGroup) {
        return k8sJobClientSelector.select(resourceGroup);
    }
}
