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
package com.oceanbase.odc.service.task.resource;

import com.oceanbase.odc.metadb.task.ResourceAllocateInfoEntity;
import com.oceanbase.odc.metadb.task.ResourceAllocateInfoRepository;
import com.oceanbase.odc.metadb.task.SupervisorEndpointEntity;
import com.oceanbase.odc.metadb.task.SupervisorEndpointRepository;
import com.oceanbase.odc.service.resource.ResourceLocation;
import com.oceanbase.odc.service.resource.ResourceManager;
import com.oceanbase.odc.service.task.supervisor.endpoint.SupervisorEndpoint;

import lombok.extern.slf4j.Slf4j;

/**
 * @author longpeng.zlp
 * @date 2024/12/2 14:43
 */
@Slf4j
public class K8STaskResourceManager extends TaskResourceManager {
    // real resource manger
    protected final ResourceManager resourceManager;

    public K8STaskResourceManager(SupervisorEndpointRepository supervisorEndpointRepository,
            ResourceAllocateInfoRepository resourceAllocateInfoRepository, ResourceManager resourceManager) {
        super(supervisorEndpointRepository, resourceAllocateInfoRepository);
        this.resourceManager = resourceManager;
    }

    /**
     * handle no resource available for allocate request allocate a new pod for request
     * 
     * @param resourceAllocateInfoEntity
     * @return info to stored
     */
    protected String handleNoResourceAvailable(ResourceAllocateInfoEntity resourceAllocateInfoEntity) {
        ResourceLocation resourceLocation = new ResourceLocation(resourceAllocateInfoEntity.getResourceRegion(),
                resourceAllocateInfoEntity.getResourceGroup());

        return null;
    }

    /**
     * detect if resource is ready for new resource
     * 
     * @param resourceAllocateInfoEntity
     * @return resource endpoint if ready, else null
     */
    protected SupervisorEndpoint detectIfResourceIsReady(ResourceAllocateInfoEntity resourceAllocateInfoEntity) {
        return null;
    }

    @Override
    protected boolean isEndpointHaveEnoughResource(SupervisorEndpointEntity supervisorEndpoint,
            ResourceAllocateInfoEntity entity) {
        // the pod should be idle, rewrite this method if needed
        return supervisorEndpoint.getLoads() == 0;
    }

    @Override
    protected void manageResource() {
        // do nothing
    }
}
