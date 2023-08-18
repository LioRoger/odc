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
package com.oceanbase.odc.server.web.trace;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.MethodParameter;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.ResponseBodyAdvice;

import com.oceanbase.odc.common.trace.TraceContextHolder;
import com.oceanbase.odc.common.util.StringUtils;
import com.oceanbase.odc.common.util.SystemUtils;
import com.oceanbase.odc.service.common.response.OdcResult;

import lombok.extern.slf4j.Slf4j;

/**
 * Attach trace info for OdcResult response
 * 
 * @author yizhou.xw
 * @version : TraceOdcResultResponseAdvice.java, v 0.1 2021-03-16 12:32
 */
@Slf4j
@ControllerAdvice(basePackages = {"com.oceanbase.odc.server.web.controller.v1"})
public class TraceOdcResultResponseAdvice implements ResponseBodyAdvice<OdcResult> {
    private static final String SERVER = SystemUtils.getHostName();

    @Autowired
    private WebTraceUtils webTraceUtils;

    @Override
    public boolean supports(MethodParameter returnType,
            Class<? extends HttpMessageConverter<?>> converterType) {
        Class<?> methodReturnType = returnType.getMethod().getReturnType();
        return OdcResult.class.isAssignableFrom(methodReturnType);
    }

    @Override
    public OdcResult beforeBodyWrite(OdcResult body, MethodParameter returnType, MediaType selectedContentType,
            Class<? extends HttpMessageConverter<?>> selectedConverterType, ServerHttpRequest request,
            ServerHttpResponse response) {
        body.setServer(SERVER);
        body.setTraceId(TraceContextHolder.getTraceId());
        if (StringUtils.isNotBlank(TraceContextHolder.getRequestId())) {
            body.setRequestId(TraceContextHolder.getRequestId());
        }
        body.setDurationMillis(TraceContextHolder.getDuration());
        body.setTimestamp(OffsetDateTime.ofInstant(
                Instant.ofEpochMilli(TraceContextHolder.getStartEpochMilli()), ZoneId.systemDefault()));
        return body;
    }
}