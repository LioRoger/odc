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

/**
 * state for resource
 * 
 * @author longpeng.zlp
 * @date 2024/8/12 11:50
 */
public enum ResourceState {
    // when submit create resource request, resource not available yet
    CREATING,
    // resource is created, compute resource available
    // task will only yield to resource with state running
    RUNNING,
    // when submit destroy resource request
    // resource will be shut down even there is task running on it
    DESTROYING,
    // resource has destroyed
    DESTROYED,
    // no available, eg machine is down or network unreachable
    UNAVAILABLE
}
