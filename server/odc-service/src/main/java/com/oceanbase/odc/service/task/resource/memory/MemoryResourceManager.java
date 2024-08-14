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
package com.oceanbase.odc.service.task.resource.memory;

import java.util.Optional;

import org.apache.commons.collections.map.LRUMap;

import com.oceanbase.odc.service.task.exception.JobException;
import com.oceanbase.odc.service.task.resource.Resource;
import com.oceanbase.odc.service.task.resource.ResourceConfig;
import com.oceanbase.odc.service.task.resource.ResourceID;
import com.oceanbase.odc.service.task.resource.ResourceManager;
import com.oceanbase.odc.service.task.resource.ResourceType;

/**
 * @author longpeng.zlp
 * @date 2024/8/13 11:28
 */
public class MemoryResourceManager implements ResourceManager {
    // hold in memory
    private final LRUMap memoryResourceMap = new LRUMap(20);

    @Override
    public Resource create(ResourceType resourceType, String jobName, ResourceConfig resourceConfig)
            throws JobException {
        MemoryResource ret = new MemoryResource();
        memoryResourceMap.put(ret.id(), ret);
        return ret;
    }

    @Override
    public Optional<? extends Resource> query(ResourceID resourceID) throws JobException {
        MemoryResource ret = (MemoryResource) memoryResourceMap.get(resourceID);
        return null == ret ? Optional.empty() : Optional.of(ret);
    }

    @Override
    public String destroy(ResourceID resourceID) throws JobException {
        memoryResourceMap.remove(resourceID);
        return resourceID.getName();
    }

    @Override
    public boolean canBeDestroyed(ResourceID resourceID) {
        // always return true
        return true;
    }
}
