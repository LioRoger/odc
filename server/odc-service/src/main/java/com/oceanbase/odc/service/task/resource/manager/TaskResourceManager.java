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
package com.oceanbase.odc.service.task.resource.manager;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;

import com.oceanbase.odc.common.json.JsonUtils;
import com.oceanbase.odc.metadb.resource.ResourceEntity;
import com.oceanbase.odc.metadb.task.ResourceAllocateInfoEntity;
import com.oceanbase.odc.metadb.task.ResourceAllocateInfoRepository;
import com.oceanbase.odc.metadb.task.SupervisorEndpointEntity;
import com.oceanbase.odc.metadb.task.SupervisorEndpointRepository;
import com.oceanbase.odc.service.resource.ResourceID;
import com.oceanbase.odc.service.resource.ResourceManager;
import com.oceanbase.odc.service.task.resource.ResourceAllocateState;
import com.oceanbase.odc.service.task.resource.ResourceUsageState;
import com.oceanbase.odc.service.task.service.TransactionManager;
import com.oceanbase.odc.service.task.supervisor.endpoint.SupervisorEndpoint;
import com.oceanbase.odc.service.task.supervisor.protocol.TaskCommandSender;
import com.oceanbase.odc.service.task.supervisor.proxy.RemoteTaskSupervisorProxy;

import lombok.extern.slf4j.Slf4j;

/**
 * @author longpeng.zlp
 * @date 2024/12/2 14:43
 */
@Slf4j
public class TaskResourceManager {
    protected final ResourceAllocator resourceAllocator;
    protected final SupervisorEndpointRepositoryWrap supervisorEndpointRepositoryWrap;
    protected final ResourceAllocateInfoRepositoryWrap resourceAllocateInfoRepositoryWrap;
    protected final RemoteTaskSupervisorProxy remoteTaskSupervisorProxy;
    protected final ResourceManageStrategy resourceManageStrategy;
    protected final ResourceDeAllocator resourceDeAllocator;

    public TaskResourceManager(SupervisorEndpointRepository supervisorEndpointRepository,
            ResourceAllocateInfoRepository resourceAllocateInfoRepository,
            ResourceManageStrategy resourceManageStrategy) {
        this.remoteTaskSupervisorProxy = new RemoteTaskSupervisorProxy(new TaskCommandSender());
        this.supervisorEndpointRepositoryWrap = new SupervisorEndpointRepositoryWrap(supervisorEndpointRepository);
        this.resourceAllocateInfoRepositoryWrap =
                new ResourceAllocateInfoRepositoryWrap(resourceAllocateInfoRepository);
        this.resourceManageStrategy = resourceManageStrategy;
        this.resourceAllocator = new ResourceAllocator(supervisorEndpointRepositoryWrap, resourceAllocateInfoRepositoryWrap, remoteTaskSupervisorProxy, resourceManageStrategy);
        this.resourceDeAllocator = new ResourceDeAllocator(supervisorEndpointRepositoryWrap, resourceAllocateInfoRepositoryWrap, remoteTaskSupervisorProxy, resourceManageStrategy);
    }

    /**
     * a loop for task resource operation
     */
    public void execute(TransactionManager transactionManager) {
        log.debug("begin task resource execute");
        // 1. allocate supervisor agent
        resourceAllocator.allocateSupervisorAgent(transactionManager);
        // 2. deallocate supervisor agent
        resourceDeAllocator.deAllocateSupervisorAgent(transactionManager);
        // 3. try scan endpoint to release
        scanEndpointsToRelease(transactionManager);
        // 4. detect if preparing resource has ready
        detectPreparingResource(transactionManager);
    }

    /**
     * detect if resource has ready
     * @param transactionManager
     */
    protected void detectPreparingResource(TransactionManager transactionManager) {
        List<SupervisorEndpointEntity> endpointEntityList =
            supervisorEndpointRepositoryWrap.collectPreparingSupervisorEndpoint();
        for (SupervisorEndpointEntity endpoint : endpointEntityList) {
            try {
                transactionManager.doInTransactionWithoutResult(() -> detectIfResourceIsReady(endpoint));
            } catch (Throwable e) {
                log.warn("detect preparing endpoint = {} failed", endpoint, e);
            }
        }
    }

    protected void detectIfResourceIsReady(SupervisorEndpointEntity entity) {
        SupervisorEndpoint endpoint = entity.getEndpoint();
        if (remoteTaskSupervisorProxy.isSupervisorAlive(endpoint)) {
            // ready set status to AVAILABLE
            supervisorEndpointRepositoryWrap.onlineSupervisorEndpoint(entity);
        } else {
            // expired, release resource and abandon supervisor endpoint
            if (isSupervisorEndpointExpired(entity)) {
                log.debug("supervisor detect alive timeout, endpoint = {}, release it", endpoint);
                resourceManageStrategy.releaseResourceById(entity);
                supervisorEndpointRepositoryWrap.offSupervisorEndpoint(entity);
            }
            log.debug("supervisor not alive yet, endpoint = {}", endpoint);
        }
    }

    protected boolean isSupervisorEndpointExpired(SupervisorEndpointEntity entity) {
        Duration between = Duration.between(entity.getUpdateTime().toInstant(), Instant.now());
        // 300 seconds considered as timeout
        // TODO(lx): config it
        return (between.toMillis() / 1000 > 360);
    }


    protected void scanEndpointsToRelease(TransactionManager transactionManager) {
        List<SupervisorEndpointEntity> endpointEntityList =
            supervisorEndpointRepositoryWrap.collectIdleAvailableSupervisorEndpoint();
        List<SupervisorEndpointEntity> toReleased = resourceManageStrategy.pickReleasedEndpoint(endpointEntityList);
        for (SupervisorEndpointEntity endpoint : toReleased) {
            try {
                transactionManager.doInTransactionWithoutResult(() -> resourceManageStrategy.releaseResourceById(endpoint));
            } catch (Throwable e) {
                log.warn("release endpoint = {} failed", endpoint, e);
            }
        }
    }

}
