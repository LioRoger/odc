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
 * resource config
 * 
 * @author longpeng.zlp
 * @date 2024/8/12 14:42
 */

public interface ResourceConfig {
    /**
     * cpu core expected for running task
     */
    double cpuCore();

    /**
     * memory size in MB expected for running task
     */
    long memInMB();

    /**
     * group of the resource
     */
    String resourceGroup();

}
