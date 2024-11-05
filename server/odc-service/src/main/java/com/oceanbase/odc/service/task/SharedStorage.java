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
package com.oceanbase.odc.service.task;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

/**
 * @author longpeng.zlp
 * @date 2024/10/24 14:52
 */
public interface SharedStorage {

    /**
     * if the shared storage is available to user
     * 
     * @return
     */
    boolean available();

    /**
     * create input stream by storage object id
     * 
     * @param objectId in shared storage
     * @return
     */
    InputStream download(String objectId) throws IOException;

    /**
     * upload permanent file to shared storage
     * 
     * @param expectName
     * @param localFile
     * @return object id for shared storage
     */
    String upload(String expectName, File localFile) throws IOException;

    /**
     * upload expired file to shared storage
     * 
     * @param expectName
     * @param localFile
     * @return object id for shared storage
     */
    String uploadTemp(String expectName, File localFile) throws IOException;


    /**
     * translate objectId to download url
     * 
     * @param objectId
     * @return
     */
    URL getDownloadURL(String objectId) throws IOException;

    /**
     * get prefix path on shared storage
     * 
     * @return
     */
    String getPathPrefix();
}
