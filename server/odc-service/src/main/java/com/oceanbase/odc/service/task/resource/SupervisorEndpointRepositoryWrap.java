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

import java.util.List;
import java.util.Optional;

import org.springframework.data.domain.PageRequest;
import org.springframework.data.jpa.domain.Specification;

import com.oceanbase.odc.common.jpa.SpecificationUtil;
import com.oceanbase.odc.metadb.task.SupervisorEndpointEntity;
import com.oceanbase.odc.metadb.task.SupervisorEndpointRepository;
import com.oceanbase.odc.service.task.supervisor.SupervisorEndpointState;
import com.oceanbase.odc.service.task.supervisor.endpoint.SupervisorEndpoint;

import lombok.extern.slf4j.Slf4j;

/**
 * @author longpeng.zlp
 * @date 2024/12/19 14:37
 */
@Slf4j
public class SupervisorEndpointRepositoryWrap {
    protected final SupervisorEndpointRepository repository;

    public SupervisorEndpointRepositoryWrap(SupervisorEndpointRepository repository) {
        this.repository = repository;
    }


    public void releaseLoad(SupervisorEndpoint supervisorEndpoint) {
        Optional<SupervisorEndpointEntity> optionalSupervisorEndpointEntity = repository
                .findByHostAndPort(supervisorEndpoint.getHost(), Integer.valueOf(supervisorEndpoint.getPort()));
        if (!optionalSupervisorEndpointEntity.isPresent()) {
            log.warn("update supervisor endpoint failed, endpoint={}", supervisorEndpoint);
            return;
        }
        SupervisorEndpointEntity supervisorEndpointEntity = optionalSupervisorEndpointEntity.get();
        operateLoad(supervisorEndpointEntity.getHost(),
                supervisorEndpointEntity.getPort(), -1);
    }

    public void operateLoad(String host, int port, int delta) {
        repository.addLoadByHostAndPort(host, port, delta);
    }

    public List<SupervisorEndpointEntity> collectAvailableSupervisorEndpoint(String region, String group) {
        Specification<SupervisorEndpointEntity> condition = Specification.where(
                SpecificationUtil.columnEqual("status", SupervisorEndpointState.AVAILABLE.name()));
        condition = condition.and(SpecificationUtil.columnEqual("resourceRegion", region))
                .and(SpecificationUtil.columnEqual("resourceGroup", group));
        return repository.findAll(condition, PageRequest.of(0, 100)).getContent();
    }
}
