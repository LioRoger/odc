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

package com.oceanbase.odc.plugin.task.api.datatransfer;

import com.oceanbase.odc.plugin.task.api.datatransfer.model.ObjectStatus;
import com.oceanbase.odc.plugin.task.api.datatransfer.model.TransferObjectsInfo;

/**
 * @author liuyizhuo.lyz
 * @date 2023-09-15
 */
public interface DataTransferTask {

    /**
     * transfer data, this is a synchronous process.
     *
     * @return transfer result, including data objects and schema objects. The two are independent of
     *         each other. An object can appear in two lists at the same time. The
     *         {@link ObjectStatus#getExportPaths()} of each object cannot be null.
     * @see DataTransferTask#getStatus()
     */
    TransferObjectsInfo transfer() throws Exception;

    /**
     * get current task status for monitoring.
     * 
     * @return transfer result, including data objects and schema objects. The two are independent of
     *         each other. An object can appear in two lists at the same time.
     */
    TransferObjectsInfo getStatus();

    /**
     * @return current progress percentage
     */
    double getProgress();

}