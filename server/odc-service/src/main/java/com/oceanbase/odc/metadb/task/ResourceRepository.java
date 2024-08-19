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
package com.oceanbase.odc.metadb.task;

import java.util.Optional;

import javax.transaction.Transactional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.oceanbase.odc.service.task.resource.ResourceID;

/**
 * jdbc query for task_resource table
 * 
 * @author longpeng.zlp
 * @date 2024/8/14 17:51
 */
public interface ResourceRepository extends JpaRepository<ResourceEntity, Long>,
        JpaSpecificationExecutor<ResourceEntity> {
    @Transactional
    @Query(value = "update task_resource set "
            + " status=:status ,"
            + " where group = :#{#resourceID.getGroup()} and name = :#{#resourceID.getName()}", nativeQuery = true)
    @Modifying
    int updateResourceStatus(@Param("resourceID") ResourceID resourceID, @Param("status") String resourceState);

    @Transactional
    @Query(value = "delete from task_resource  "
            + " where group = :#{#resourceID.getGroup()} and name = :#{#resourceID.getName()} limit 1",
            nativeQuery = true)
    @Modifying
    int deleteResource(@Param("resourceID") ResourceID resourceID);

    @Query(value = "SELECT * FROM task_resource WHERE group = :#{#resourceID.getGroup()} and name = :#{#resourceID.getName()}",
            nativeQuery = true)
    Optional<ResourceEntity> findByResourceID(@Param("param") ResourceID resourceID);
}
