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

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;

import com.oceanbase.odc.common.json.JsonUtils;
import com.oceanbase.odc.common.util.StringUtils;
import com.oceanbase.odc.metadb.task.ResourceAllocateInfoEntity;
import com.oceanbase.odc.metadb.task.ResourceAllocateInfoRepository;
import com.oceanbase.odc.metadb.task.SupervisorEndpointEntity;
import com.oceanbase.odc.metadb.task.SupervisorEndpointRepository;
import com.oceanbase.odc.service.task.supervisor.endpoint.SupervisorEndpoint;
import com.oceanbase.odc.service.task.supervisor.protocol.TaskCommandSender;
import com.oceanbase.odc.service.task.supervisor.proxy.RemoteTaskSupervisorProxy;

import lombok.extern.slf4j.Slf4j;

/**
 * @author longpeng.zlp
 * @date 2024/12/2 14:43
 */
@Slf4j
public abstract class TaskResourceManager {
    protected final SupervisorEndpointRepositoryWrap supervisorEndpointRepositoryWrap;
    protected final ResourceAllocateInfoRepositoryWrap resourceAllocateInfoRepositoryWrap;
    protected final RemoteTaskSupervisorProxy remoteTaskSupervisorProxy;

    public TaskResourceManager(SupervisorEndpointRepository supervisorEndpointRepository,
            ResourceAllocateInfoRepository resourceAllocateInfoRepository) {
        this.supervisorEndpointRepositoryWrap = new SupervisorEndpointRepositoryWrap(supervisorEndpointRepository);
        this.resourceAllocateInfoRepositoryWrap =
                new ResourceAllocateInfoRepositoryWrap(resourceAllocateInfoRepository);
        this.remoteTaskSupervisorProxy = new RemoteTaskSupervisorProxy(new TaskCommandSender());
    }

    /**
     * a loop for task resource operation
     */
    public void execute() {
        log.debug("begin task resource execute");
        // 1. allocate supervisor agent
        allocateSupervisorAgent();
        // 2. deallocate supervisor agent
        deAllocateSupervisorAgent();
        // 3. check if there are resource need be released or what ever
        manageResource();
    }

    protected void allocateSupervisorAgent() {
        List<ResourceAllocateInfoEntity> resourceToAllocate = resourceAllocateInfoRepositoryWrap.collectAllocateInfo();
        for (ResourceAllocateInfoEntity resourceAllocateInfo : resourceToAllocate) {
            if (!isAllocateInfoValid(resourceAllocateInfo)) {
                continue;
            }
            ResourceAllocateState resourceAllocateState =
                    ResourceAllocateState.fromString(resourceAllocateInfo.getResourceAllocateState());
            if (resourceAllocateState == ResourceAllocateState.PREPARING) {
                handleAllocateResourceInfoInPreparing(resourceAllocateInfo);
            } else if (resourceAllocateState == ResourceAllocateState.CREATING_RESOURCE) {
                handleAllocateResourceInfoInCreatingResource(resourceAllocateInfo);
            } else {
                throw new RuntimeException(
                        "invalid state for allocate supervisor agent, current is " + resourceAllocateInfo);
            }
        }
    }

    // handle prepare allocate resource state
    protected void handleAllocateResourceInfoInPreparing(ResourceAllocateInfoEntity allocateInfoEntity) {
        SupervisorEndpoint supervisorEndpoint = chooseSupervisorEndpoint(allocateInfoEntity);
        if (null != supervisorEndpoint) {
            // allocate success
            log.info("allocate supervisor endpoint = {} for job id = {}", supervisorEndpoint,
                    allocateInfoEntity.getTaskId());
            resourceAllocateInfoRepositoryWrap.allocateForJob(supervisorEndpoint, allocateInfoEntity.getTaskId());
        } else {
            // try allocate new resource
            log.info("not endpoint available for job id = {}, try allocate new resource",
                    allocateInfoEntity.getTaskId());
            String allocateInfo = handleNoResourceAvailable(allocateInfoEntity);
            // no info provided, not change current state
            if (StringUtils.isNotBlank(allocateInfo)) {
                resourceAllocateInfoRepositoryWrap.prepareResourceForJob(allocateInfo, allocateInfoEntity.getTaskId());
            }
        }
    }

    // handle creating resource allocate resource state
    protected void handleAllocateResourceInfoInCreatingResource(ResourceAllocateInfoEntity allocateInfoEntity) {
        SupervisorEndpoint supervisorEndpoint = detectIfResourceIsReady(allocateInfoEntity);
        if (null != supervisorEndpoint) {
            // allocate success
            log.info("resource ready with info = {}, allocate supervisor endpoint = {} for job id = {}",
                    allocateInfoEntity.getResourceCreateInfo(), supervisorEndpoint, allocateInfoEntity.getTaskId());
            resourceAllocateInfoRepositoryWrap.allocateForJob(supervisorEndpoint, allocateInfoEntity.getTaskId());
        } else {
            // wait resource ready
            log.info("resource not ready with info = {} for job id = {}, wait resource ready",
                    allocateInfoEntity.getResourceCreateInfo(), allocateInfoEntity.getTaskId());
        }
    }

    // if resource entity is valid
    protected boolean isAllocateInfoValid(ResourceAllocateInfoEntity allocateInfoEntity) {
        if (isAllocateInfoExpired(allocateInfoEntity)) {
            log.info("resource prepare expired for entity =  {}", allocateInfoEntity);
            resourceAllocateInfoRepositoryWrap.failedAllocateForId(allocateInfoEntity.getTaskId());
            return false;
        }
        // resource not need any more
        if (ResourceUsageState.fromString(allocateInfoEntity.getResourceUsageState()) == ResourceUsageState.FINISHED) {
            log.info("resource prepare canceled for entity = {}", allocateInfoEntity);
            resourceAllocateInfoRepositoryWrap.finishedAllocateForId(allocateInfoEntity.getTaskId());
            return false;
        }
        return true;
    }

    protected void deAllocateSupervisorAgent() {
        List<ResourceAllocateInfoEntity> resourceToDeallocate =
                resourceAllocateInfoRepositoryWrap.collectDeAllocateInfo();
        for (ResourceAllocateInfoEntity deAllocateInfoEntity : resourceToDeallocate) {
            SupervisorEndpoint supervisorEndpoint =
                    JsonUtils.fromJson(deAllocateInfoEntity.getEndpoint(), SupervisorEndpoint.class);
            // allocate success
            if (null != supervisorEndpoint) {
                supervisorEndpointRepositoryWrap.releaseLoad(supervisorEndpoint);
            } else {
                log.warn("supervisorEndpoint not parsed, taskID = {}, originValue = {}",
                        deAllocateInfoEntity.getTaskId(), deAllocateInfoEntity.getEndpoint());
            }
            resourceAllocateInfoRepositoryWrap.finishedAllocateForId(deAllocateInfoEntity.getTaskId());
        }
    }

    /**
     * try choose a supervisor agent for given region and group
     * 
     * @return
     */
    protected SupervisorEndpoint chooseSupervisorEndpoint(ResourceAllocateInfoEntity entity) {
        List<SupervisorEndpointEntity> supervisorEndpointEntities = supervisorEndpointRepositoryWrap
                .collectAvailableSupervisorEndpoint(entity.getResourceRegion(), entity.getResourceGroup());
        // no available found
        if (CollectionUtils.isEmpty(supervisorEndpointEntities)) {
            // no endpoint found, that's not good
            log.warn("not supervisor end point found");
            return null;
        }
        // use load smaller
        supervisorEndpointEntities = supervisorEndpointEntities.stream()
                .sorted((s1, s2) -> Integer.compare(s1.getLoads(), s2.getLoads())).collect(
                        Collectors.toList());
        for (SupervisorEndpointEntity tmp : supervisorEndpointEntities) {
            if (!isEndpointHaveEnoughResource(tmp, entity)) {
                continue;
            }
            SupervisorEndpoint ret = new SupervisorEndpoint(tmp.getHost(), tmp.getPort());
            // TODO(longxuan): handle unreached supervisor
            if (remoteTaskSupervisorProxy.isSupervisorAlive(ret)) {
                // each task means one load
                supervisorEndpointRepositoryWrap.operateLoad(tmp.getHost(), tmp.getPort(), 1);
                return ret;
            }
        }
        return null;
    }

    protected boolean isAllocateInfoExpired(ResourceAllocateInfoEntity entity) {
        Duration between = Duration.between(entity.getUpdateTime().toInstant(), Instant.now());
        // 300 seconds considered as timeout
        // TODO(lx): config it
        return (between.toMillis() / 1000 > 60);
    }


    /**
     * handle no resource available for allocate request
     * 
     * @param resourceAllocateInfoEntity
     * @return info to stored
     */
    protected abstract String handleNoResourceAvailable(ResourceAllocateInfoEntity resourceAllocateInfoEntity);

    /**
     * detect if resource is ready for new resource
     * 
     * @param resourceAllocateInfoEntity
     * @return resource endpoint if ready, else null
     */
    protected abstract SupervisorEndpoint detectIfResourceIsReady(
            ResourceAllocateInfoEntity resourceAllocateInfoEntity);

    /**
     * if supervisorEndpoint have enough resource for allocate request
     * 
     * @param supervisorEndpoint
     * @return
     */
    protected abstract boolean isEndpointHaveEnoughResource(SupervisorEndpointEntity supervisorEndpoint,
            ResourceAllocateInfoEntity entity);

    /**
     * manager resource, do real resource related operation
     */
    protected abstract void manageResource();

}
