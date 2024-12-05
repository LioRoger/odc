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
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.jpa.domain.Specification;

import com.oceanbase.odc.common.jpa.SpecificationUtil;
import com.oceanbase.odc.common.json.JsonUtils;
import com.oceanbase.odc.metadb.task.ResourceAllocateInfoEntity;
import com.oceanbase.odc.metadb.task.ResourceAllocateInfoRepository;
import com.oceanbase.odc.metadb.task.SupervisorEndpointEntity;
import com.oceanbase.odc.metadb.task.SupervisorEndpointRepository;
import com.oceanbase.odc.service.task.supervisor.SupervisorEndpointState;
import com.oceanbase.odc.service.task.supervisor.endpoint.SupervisorEndpoint;
import com.oceanbase.odc.service.task.util.TaskSupervisorUtil;

import lombok.extern.slf4j.Slf4j;

/**
 * @author longpeng.zlp
 * @date 2024/12/2 14:43
 */
@Slf4j
public class ProcessTaskResourceManager implements TaskResourceManager {
    protected final SupervisorEndpointRepository supervisorEndpointRepository;
    protected final ResourceAllocateInfoRepository resourceAllocateInfoRepository;


    public ProcessTaskResourceManager(SupervisorEndpointRepository supervisorEndpointRepository,
            ResourceAllocateInfoRepository resourceAllocateInfoRepository) {
        this.supervisorEndpointRepository = supervisorEndpointRepository;
        this.resourceAllocateInfoRepository = resourceAllocateInfoRepository;
    }

    @Override
    public void initTaskResourceManager() {
        tryRegisterTaskSupervisorAgent();
    }

    /**
     */
    @Override
    public void execute() {
        log.debug("begin process task resource execute");
        // 1. allocate supervisor agent
        allocateSupervisorAgent();
        // 2. deallocate supervisor agent
        deAllocateSupervisorAgent();
    }

    protected void allocateSupervisorAgent() {
        List<ResourceAllocateInfoEntity> resourceToAllocate = collectAllocateInfo();
        for (ResourceAllocateInfoEntity allocateInfoEntity : resourceToAllocate) {
            if (isAllocateInfoExpired(allocateInfoEntity)) {
                failedAllocateForId(allocateInfoEntity.getTaskId());
            }
            SupervisorEndpoint supervisorEndpoint = chooseSupervisorEndpoint(supervisorEndpointRepository);
            // allocate success
            if (null != supervisorEndpoint) {
                allocateForJob(supervisorEndpoint, allocateInfoEntity.getTaskId());
            }
        }
    }

    protected void deAllocateSupervisorAgent() {
        List<ResourceAllocateInfoEntity> resourceToDeallocate = collectDeAllocateInfo();
        for (ResourceAllocateInfoEntity deAllocateInfoEntity : resourceToDeallocate) {
            SupervisorEndpoint supervisorEndpoint =
                    JsonUtils.fromJson(deAllocateInfoEntity.getEndpoint(), SupervisorEndpoint.class);
            // allocate success
            if (null != supervisorEndpoint) {
                releaseLoad(supervisorEndpoint);
            } else {
                log.warn("supervisorEndpoint not parsed, taskID = {}, originValue = {}",
                        deAllocateInfoEntity.getTaskId(), deAllocateInfoEntity.getEndpoint());
            }
            finishedAllocateForId(deAllocateInfoEntity.getTaskId());
        }
    }

    protected boolean isAllocateInfoExpired(ResourceAllocateInfoEntity entity) {
        Duration between = Duration.between(entity.getUpdateTime().toInstant(), Instant.now());
        // 300 seconds considered as timeout
        // TODO(lx): config it
        return (between.toMillis() / 1000 > 300);
    }


    private void releaseLoad(SupervisorEndpoint supervisorEndpoint) {
        Optional<SupervisorEndpointEntity> optionalSupervisorEndpointEntity = supervisorEndpointRepository
                .findByHostAndPort(supervisorEndpoint.getHost(), Integer.valueOf(supervisorEndpoint.getPort()));
        if (!optionalSupervisorEndpointEntity.isPresent()) {
            log.warn("update supervisor endpoint failed, endpoint={}", supervisorEndpoint);
            return;
        }
        SupervisorEndpointEntity supervisorEndpointEntity = optionalSupervisorEndpointEntity.get();
        supervisorEndpointRepository.addLoadByHostAndPort(supervisorEndpointEntity.getHost(),
                supervisorEndpointEntity.getPort(), -1);
    }

    /**
     * collect allocate info needed to allocate
     * 
     * @return
     */
    protected List<ResourceAllocateInfoEntity> collectAllocateInfo() {
        Specification<ResourceAllocateInfoEntity> condition = Specification.where(
                SpecificationUtil.columnEqual("resourceAllocateState", ResourceAllocateState.PREPARING.name()));
        return resourceAllocateInfoRepository.findAll(condition, PageRequest.of(0, 100)).getContent();
    }

    /**
     * collect deallocate info needed to deallocate
     * 
     * @return
     */
    protected List<ResourceAllocateInfoEntity> collectDeAllocateInfo() {
        Specification<ResourceAllocateInfoEntity> condition = Specification.where(
                SpecificationUtil.columnEqual("resourceUsageState", ResourceUsageState.FINISHED.name()));
        Specification<ResourceAllocateInfoEntity> query = condition
                .and(SpecificationUtil.columnEqual("resourceAllocateState", ResourceAllocateState.AVAILABLE.name()));
        return resourceAllocateInfoRepository.findAll(query, PageRequest.of(0, 100)).getContent();
    }

    protected List<SupervisorEndpointEntity> collectRunningSupervisorEndpoint() {
        Specification<SupervisorEndpointEntity> condition = Specification.where(
                SpecificationUtil.columnEqual("status", SupervisorEndpointState.AVAILABLE.name()));
        return supervisorEndpointRepository.findAll(condition, PageRequest.of(0, 100)).getContent();
    }

    /**
     * this logic will wrap to a interface
     * 
     * @return
     */
    protected SupervisorEndpoint chooseSupervisorEndpoint(SupervisorEndpointRepository supervisorEndpointRepository) {
        List<SupervisorEndpointEntity> supervisorEndpointEntities = collectRunningSupervisorEndpoint();
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
        SupervisorEndpointEntity tmp = supervisorEndpointEntities.get(0);
        SupervisorEndpoint ret = new SupervisorEndpoint(tmp.getHost(), tmp.getPort());
        // each task means one load
        supervisorEndpointRepository.addLoadByHostAndPort(tmp.getHost(), tmp.getPort(), 1);
        return ret;
    }

    /**
     * register self to meta store
     */
    private void tryRegisterTaskSupervisorAgent() {
        log.info("start with supervisor agent mode, try register agent");
        SupervisorEndpoint localEndpoint = TaskSupervisorUtil.getDefaultSupervisorEndpoint();
        Optional<SupervisorEndpointEntity> registered = supervisorEndpointRepository
                .findByHostAndPort(localEndpoint.getHost(), Integer.valueOf(localEndpoint.getPort()));
        if (registered.isPresent()) {
            supervisorEndpointRepository.updateStatusByHostAndPort(localEndpoint.getHost(), localEndpoint.getPort(),
                    SupervisorEndpointState.AVAILABLE.name());
        } else {
            SupervisorEndpointEntity created = new SupervisorEndpointEntity();
            created.setHost(localEndpoint.getHost());
            created.setPort(localEndpoint.getPort());
            created.setResourceID(-1L);
            created.setLoads(0);
            created.setStatus(SupervisorEndpointState.AVAILABLE.name());
            created.setCreateTime(new Date(System.currentTimeMillis()));
            supervisorEndpointRepository.save(created);
        }
    }


    /**
     * allocate endpoint for id, this method will called by resource manager
     * 
     * @param supervisorEndpoint
     * @param taskID
     */
    protected void allocateForJob(SupervisorEndpoint supervisorEndpoint, Long taskID) {
        resourceAllocateInfoRepository.updateEndpointByTaskId(JsonUtils.toJson(supervisorEndpoint), taskID);
    }

    /**
     * task allocate has failed for id, this method will called by resource manager
     * 
     * @param taskID
     */
    protected void failedAllocateForId(Long taskID) {
        resourceAllocateInfoRepository.updateResourceAllocateStateByTaskId(ResourceAllocateState.FAILED.name(), taskID);
    }

    /**
     * task allocate has finished for id, this method will called by resource manager
     * 
     * @param taskID
     */
    protected void finishedAllocateForId(Long taskID) {
        resourceAllocateInfoRepository.updateResourceAllocateStateByTaskId(ResourceAllocateState.FINISHED.name(),
                taskID);
    }
}
