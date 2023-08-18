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
package com.oceanbase.odc.service.common.response;

import java.time.OffsetDateTime;

import org.springframework.http.HttpStatus;

import lombok.Data;

/**
 * @author yizhou.xw
 * @version : BaseResponse.java, v 0.1 2021-02-19 14:49
 */
@Data
public class BaseResponse {
    /**
     * 请求是否处理成功
     */
    private Boolean successful;

    private HttpStatus httpStatus;

    /**
     * 请求处理时间戳
     */
    private OffsetDateTime timestamp;

    /**
     * 请求处理耗时（毫秒）
     */
    private Long durationMillis;

    /**
     * traceId, generated by odc server
     */
    private String traceId;

    /**
     * requestId，often set from client side, e.g. X-REQUEST-ID header
     */
    private String requestId;

    /**
     * 处理请求的 server 节点标识，可能是 ip:port，也可能是 docker名称
     */
    private String server;
}