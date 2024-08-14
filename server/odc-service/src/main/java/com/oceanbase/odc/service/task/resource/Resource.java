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

import java.sql.Date;

/**
 * Resource is a compute holder for task or function It may be local memory or remote machine.
 * 
 * @author longpeng.zlp
 * @date 2024/8/12 10:53
 */
public interface Resource {
    /**
     * resource ID
     */
    ResourceID id();

    /**
     * resource type
     */
    ResourceType type();

    /**
     * end point
     */
    ResourceEndPoint endpoint();

    /**
     * create date of the resource
     * 
     * @return
     */
    Date createDate();
}
