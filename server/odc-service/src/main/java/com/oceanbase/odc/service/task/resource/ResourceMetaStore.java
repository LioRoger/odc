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
 * meta store of the resource
 * 
 * @author longpeng.zlp
 * @date 2024/8/12 11:32
 */
public interface ResourceMetaStore {
    /**
     * find if the resource has created
     * 
     * @param resourceID
     * @return
     */
    Resource findResource(ResourceID resourceID);

    /**
     * create the resource. Exception will throw if Resource has been created
     * 
     * @param resource
     * @return
     */
    void createResource(Resource resource) throws Exception;

    /**
     * update resource
     * 
     * @param resource
     * @return previous resource info
     */
    Resource updateResource(Resource resource);

    /**
     * remove the resource with resourceID
     * 
     * @param resourceID
     * @return true if resource exists and been removed. false if resource not exist
     */
    boolean deleteResource(ResourceID resourceID);
}
