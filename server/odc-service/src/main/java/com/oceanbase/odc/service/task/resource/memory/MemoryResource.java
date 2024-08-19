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

import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

import com.oceanbase.odc.service.task.resource.Resource;
import com.oceanbase.odc.service.task.resource.ResourceEndPoint;
import com.oceanbase.odc.service.task.resource.ResourceID;
import com.oceanbase.odc.service.task.resource.ResourceMode;
import com.oceanbase.odc.service.task.resource.ResourceState;

/**
 * resource in memory
 * 
 * @author longpeng.zlp
 * @date 2024/8/13 10:25
 */
public class MemoryResource implements Resource {
    private static final AtomicLong ALLOCATE_SEQ = new AtomicLong(0);
    private final long seq = ALLOCATE_SEQ.getAndIncrement();
    private final Date createDate = new Date(System.currentTimeMillis());

    @Override
    public ResourceID id() {
        return new ResourceID("default", "memory:" + seq);
    }

    @Override
    public ResourceMode type() {
        return ResourceMode.MEMORY;
    }

    @Override
    public ResourceEndPoint endpoint() {
        return new ResourceEndPoint("memory");
    }

    @Override
    public ResourceState resourceState() {
        return ResourceState.RUNNING;
    }

    @Override
    public Date createDate() {
        return createDate;
    }
}
