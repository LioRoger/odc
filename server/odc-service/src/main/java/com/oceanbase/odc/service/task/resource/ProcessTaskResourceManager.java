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

package com.oceanbase.odc.service.task.resource;

import java.util.Optional;

import com.oceanbase.odc.metadb.task.SupervisorEndpointRepository;
import com.oceanbase.odc.service.task.caller.JobContext;
import com.oceanbase.odc.service.task.supervisor.endpoint.SupervisorEndpoint;

/**
 * @author longpeng.zlp
 * @date 2024/12/2 14:43
 */
public class ProcessTaskResourceManager implements TaskResourceManager {
    private final SupervisorEndpointRepository supervisorEndpointRepository;

    public ProcessTaskResourceManager(SupervisorEndpointRepository supervisorEndpointRepository) {
        this.supervisorEndpointRepository = supervisorEndpointRepository;
    }

    @Override
    public Optional<SupervisorEndpoint> tryAllocateSupervisorEndpoint(JobContext jobContext) {
        return Optional.empty();
    }

    @Override
    public void deallocateSupervisorEndpoint(JobContext jobContext) {

    }
}
