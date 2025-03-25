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
package com.oceanbase.odc.service.resource.k8s;

import com.oceanbase.odc.common.util.StringUtils;
import com.oceanbase.odc.service.resource.Resource;
import com.oceanbase.odc.service.resource.ResourceLocation;
import com.oceanbase.odc.service.resource.ResourceManager;
import com.oceanbase.odc.service.resource.ResourceWithID;
import com.oceanbase.odc.service.task.config.K8sProperties;
import com.oceanbase.odc.service.task.resource.Constants;
import com.oceanbase.odc.service.task.resource.K8sPodResource;
import com.oceanbase.odc.service.task.resource.K8sResourceContext;
import com.oceanbase.odc.service.task.resource.manager.strategy.k8s.K8sResourceContextBuilder;

/**
 * @author longpeng.zlp
 * @date 2025/3/19 17:05
 */
public class K8sResourceUtil {

    public static ResourceWithID<K8sPodResource> createK8sPodResource(
            ResourceManager resourceManager, ResourceLocation resourceLocation, K8sProperties k8sProperties, long id,
            int supervisorListenPort)
            throws Exception {
        K8sResourceContextBuilder contextBuilder = new K8sResourceContextBuilder(k8sProperties, supervisorListenPort);
        K8sResourceContext k8sResourceContext =
                contextBuilder.buildK8sResourceContext(id, resourceLocation);
        ResourceWithID<K8sPodResource> k8sPodResource = null;
        // allocate resource failed, send alarm event and throws exception
        try {
            k8sPodResource = resourceManager.create(resourceLocation,
                    k8sResourceContext);
        } catch (Exception e) {
            throw e;
        }
        K8sPodResource podResource = k8sPodResource.getResource();
        podResource.setServicePort(String.valueOf(supervisorListenPort));
        if (StringUtils.isEmpty(podResource.getPodIpAddress())) {
            podResource.setPodIpAddress(Constants.RESOURCE_NULL_HOST);
        }
        return k8sPodResource;
    }

    public static String queryIpAndAddress(ResourceManager resourceManager, long resourceID) throws Exception {
        ResourceWithID<Resource> resourceWithID = resourceManager.query(resourceID)
                .orElseThrow(() -> new RuntimeException("resource not found, id = " + resourceID));
        K8sPodResource podResource = (K8sPodResource) resourceWithID.getResource();
        return podResource.getPodIpAddress();
    }
}
