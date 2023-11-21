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

package com.oceanbase.odc.plugin.task.mysql.datatransfer.job.datax.model.parameter;

import java.util.List;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class TxtWriterPluginParameter extends TxtPluginParameter {

    /*
     * Clean all files with the fileName prefix in the directory before writing.
     */
    private String writeMode = "truncate";
    private String fileFormat;
    private String path;
    private String fileName;
    private List<String> header;
    private String lineDelimiter;
    /**
     * for sql format
     */
    private String table;
    private String quoteChar;
    private int commitSize;

    private DataXCsvConfig csvWriterConfig;

}