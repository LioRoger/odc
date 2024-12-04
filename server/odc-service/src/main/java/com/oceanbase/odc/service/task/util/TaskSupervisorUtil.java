/*
 * Copyright (c) 2024 OceanBase.
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

package com.oceanbase.odc.service.task.util;

import java.util.List;
import java.util.Optional;

import org.springframework.boot.autoconfigure.web.ServerProperties;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.jpa.domain.Specification;

import com.oceanbase.odc.common.jpa.SpecificationUtil;
import com.oceanbase.odc.common.util.SystemUtils;
import com.oceanbase.odc.metadb.task.SupervisorEndpointEntity;
import com.oceanbase.odc.metadb.task.SupervisorEndpointRepository;
import com.oceanbase.odc.service.common.util.SpringContextUtil;
import com.oceanbase.odc.service.task.config.TaskFrameworkProperties;
import com.oceanbase.odc.service.task.enums.TaskRunMode;
import com.oceanbase.odc.service.task.supervisor.endpoint.SupervisorEndpoint;

import lombok.extern.slf4j.Slf4j;

/**
 * @author longpeng.zlp
 * @date 2024/12/2 10:50
 */
@Slf4j
public class TaskSupervisorUtil {
    public static void releaseLoad(SupervisorEndpointRepository supervisorEndpointRepository, SupervisorEndpoint supervisorEndpoint) {
        Optional<SupervisorEndpointEntity> optionalSupervisorEndpointEntity = supervisorEndpointRepository.findByHostAndPort(supervisorEndpoint.getHost(), Integer.valueOf(supervisorEndpoint.getPort()));
        if (!optionalSupervisorEndpointEntity.isPresent()) {
            log.warn("update supervisor endpoint failed, endpoint={}", supervisorEndpoint);
            return;
        }
        SupervisorEndpointEntity supervisorEndpointEntity = optionalSupervisorEndpointEntity.get();
        supervisorEndpointRepository.addLoadByHostAndPort(supervisorEndpointEntity.getHost(), supervisorEndpointEntity.getPort(), - 1);
    }

    public static Page<SupervisorEndpointEntity> find(SupervisorEndpointRepository supervisorEndpointRepository, List<String> status, int page, int size) {
        Specification<SupervisorEndpointEntity> condition = Specification.where(
            SpecificationUtil.columnIn("status", status));
        return supervisorEndpointRepository.findAll(condition, PageRequest.of(page, size));
    }

    public static SupervisorEndpoint getDefaultSupervisorEndpoint() {
        String host = SystemUtils.getLocalIpAddress();
        ServerProperties serverProperties  = SpringContextUtil.getBean(ServerProperties.class);
        int port = serverProperties.getPort();
        return new SupervisorEndpoint(host,(port + 1000) % 65535);
    }

    public static boolean isTaskSupervisorEnabled(TaskFrameworkProperties taskFrameworkProperties) {
        return (taskFrameworkProperties.isEnableTaskSupervisorAgent() && taskFrameworkProperties.getRunMode() == TaskRunMode.PROCESS);
    }
}
