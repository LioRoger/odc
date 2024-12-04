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

package com.oceanbase.odc.service.task.supervisor;

/**
 * @author longpeng.zlp
 * @date 2024/11/29 16:18
 */
public enum SupervisorEndpointState {
    // in prepare status
    PREPARING,
    // endpoint is available
    AVAILABLE,
    // endpoint has been destroyed
    DESTROYED,
    // endpoint not reachable
    UNAVAILABLE,
    // endpoint is abandoned
    ABANDON
}
