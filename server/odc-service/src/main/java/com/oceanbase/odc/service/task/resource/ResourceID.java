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

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * global unique resource ID
 * 
 * @author longpeng.zlp
 * @date 2024/8/12 11:30
 */
@AllArgsConstructor
@Data
@EqualsAndHashCode
@ToString
public class ResourceID {
    /**
     * group of resource. eg there is cluster 1 and cluster 2.
     */
    private final String group;

    /**
     * name of resource.
     */
    private final String name;
}
