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
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import com.oceanbase.odc.TestConnectionUtil;
import com.oceanbase.odc.common.json.JsonUtils;
import com.oceanbase.odc.common.security.PasswordUtils;
import com.oceanbase.odc.core.shared.PreConditions;
import com.oceanbase.odc.core.shared.constant.ConnectType;
import com.oceanbase.odc.core.shared.constant.ErrorCodes;
import com.oceanbase.odc.core.shared.constant.TaskErrorStrategy;
import com.oceanbase.odc.core.shared.constant.TaskType;
import com.oceanbase.odc.service.common.model.HostProperties;
import com.oceanbase.odc.service.connection.model.ConnectionConfig;
import com.oceanbase.odc.service.flow.task.model.DatabaseChangeParameters;
import com.oceanbase.odc.service.objectstorage.cloud.model.ObjectStorageConfiguration;
import com.oceanbase.odc.service.objectstorage.cloud.model.ObjectStorageConfiguration.CloudProvider;
import com.oceanbase.odc.service.plugin.PluginProperties;
import com.oceanbase.odc.service.task.caller.JobContext;
import com.oceanbase.odc.service.task.caller.JobEnvBuilder;
import com.oceanbase.odc.service.task.config.DefaultJobConfiguration;
import com.oceanbase.odc.service.task.config.JobConfigurationHolder;
import com.oceanbase.odc.service.task.config.TaskFrameworkProperties;
import com.oceanbase.odc.service.task.constants.JobConstants;
import com.oceanbase.odc.service.task.constants.JobEnvKeyConstants;
import com.oceanbase.odc.service.task.constants.JobParametersKeyConstants;
import com.oceanbase.odc.service.task.enums.TaskRunModeEnum;
import com.oceanbase.odc.service.task.executor.logger.LogUtils;
import com.oceanbase.odc.service.task.executor.task.DatabaseChangeTask;
import com.oceanbase.odc.service.task.schedule.DefaultJobContextBuilder;
import com.oceanbase.odc.service.task.schedule.DefaultJobDefinition;
import com.oceanbase.odc.service.task.schedule.JobDefinition;
import com.oceanbase.odc.service.task.schedule.JobIdentity;
import com.oceanbase.odc.service.task.schedule.provider.DefaultHostUrlProvider;
import com.oceanbase.odc.service.task.schedule.provider.HostUrlProvider;
import com.oceanbase.odc.service.task.service.TaskFrameworkService;
import com.oceanbase.odc.service.task.util.JobUtils;
import com.oceanbase.odc.test.database.TestDBConfiguration;
import com.oceanbase.odc.test.database.TestDBConfigurations;
import com.oceanbase.odc.test.util.JdbcUtil;

import lombok.extern.slf4j.Slf4j;

/**
 * @author yaobin
 * @date 2024-01-22
 * @since 4.2.4
 */
@Ignore("manual test this case for process mode")
@Slf4j
public class ProcessModeTest {

    @BeforeClass
    public static void init() throws IOException {
        String key = PasswordUtils.random(32);
        String salt = PasswordUtils.random(8);
        System.setProperty(JobEnvKeyConstants.ENCRYPT_KEY, key);
        System.setProperty(JobEnvKeyConstants.ENCRYPT_SALT, salt);

        TestDBConfiguration tdc = TestDBConfigurations.getInstance().getTestOBMysqlConfiguration();
        System.setProperty(JobEnvKeyConstants.DATABASE_HOST, tdc.getHost());
        System.setProperty(JobEnvKeyConstants.DATABASE_PORT, tdc.getPort() + "");
        System.setProperty(JobEnvKeyConstants.DATABASE_NAME, tdc.getDefaultDBName());
        System.setProperty(JobEnvKeyConstants.DATABASE_USERNAME,
                JdbcUtil.buildUser(tdc.getUsername(), tdc.getTenant(), tdc.getCluster()));
        System.setProperty(JobEnvKeyConstants.DATABASE_PASSWORD, tdc.getPassword());
        System.setProperty(JobEnvKeyConstants.ODC_LOG_DIRECTORY, LogUtils.getBaseLogPath());
        System.setProperty(JobEnvKeyConstants.ODC_BOOT_MODE, JobConstants.ODC_BOOT_MODE_EXECUTOR);
        System.setProperty(JobEnvKeyConstants.ODC_TASK_RUN_MODE, TaskRunModeEnum.K8S.name());
        System.setProperty(JobEnvKeyConstants.ODC_SERVER_PORT, "8990");
        DefaultJobConfiguration jc = new DefaultJobConfiguration() {};

        HostProperties hostProperties = new HostProperties();
        hostProperties.setOdcHost("localhost");
        hostProperties.setPort("8990");
        HostUrlProvider urlProvider = new DefaultHostUrlProvider(
                () -> Mockito.mock(TaskFrameworkProperties.class), hostProperties);
        jc.setHostUrlProvider(urlProvider);
        jc.setTaskFrameworkService(Mockito.mock(TaskFrameworkService.class));
        JobConfigurationHolder.setJobConfiguration(jc);
    }

    @Test
    public void test_start_task_process_mode() {
        RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
        String processId = runtimeMxBean.getName(); // return "pid@hostname"
        String pid = processId.split("@")[0]; // get process id

        log.info("Current Java PID: {}", pid);

        ProcessBuilder pb = new ProcessBuilder();
        List<String> commands = new ArrayList<>();
        commands.add("java");
        commands.addAll(runtimeMxBean.getInputArguments().stream()
                .filter(c -> !c.startsWith("-agentlib") && !c.startsWith("-javaagent"))
                .collect(Collectors.toList()));
        commands.add("-classpath");
        commands.add(runtimeMxBean.getClassPath());
        commands.add(JobConstants.ODC_SERVER_CLASS_NAME);
        pb.directory(new File("."));

        setEnvironments(pb);
        pb.command(commands);
        try {
            Process process = pb.start();
            boolean exited = process.waitFor(60, TimeUnit.SECONDS);
            Assert.assertFalse(exited);
        } catch (Throwable ex) {
            log.error("start odc server error:", ex);
        }
    }

    private void setEnvironments(ProcessBuilder pb) {
        Map<String, String> environment = pb.environment();
        String pluginPath = Paths.get("").toAbsolutePath().getParent().getParent()
                .resolve("distribution/plugins").toFile().getAbsolutePath();
        environment.put(PluginProperties.PLUGIN_DIR_KEY, pluginPath);

        Long exceptedTaskId = System.currentTimeMillis();
        JobIdentity jobIdentity = JobIdentity.of(exceptedTaskId);
        JobDefinition jd = buildJobDefinition();
        JobContext jc = new DefaultJobContextBuilder().build(jobIdentity, jd);
        environment.putAll(new JobEnvBuilder().buildMap(jc, TaskRunModeEnum.PROCESS));
    }

    private static boolean isaBoolean() {
        return System.getProperties().elements().hasMoreElements();
    }

    private JobDefinition buildJobDefinition() {
        Long exceptedTaskId = System.currentTimeMillis();
        DatabaseChangeParameters parameters = new DatabaseChangeParameters();
        parameters.setSqlContent(String.format("CREATE TABLE %s (id int(10))", "t_" + exceptedTaskId));
        parameters.setErrorStrategy(TaskErrorStrategy.ABORT.name());
        PreConditions.validArgumentState(
                parameters.getSqlContent() != null || CollectionUtils.isNotEmpty(parameters.getSqlObjectIds()),
                ErrorCodes.BadArgument, new Object[] {"sql"}, "input sql is empty");

        ConnectionConfig config = TestConnectionUtil.getTestConnectionConfig(ConnectType.OB_MYSQL);
        Map<String, String> jobData = new HashMap<>();
        jobData.put(JobParametersKeyConstants.META_TASK_PARAMETER_JSON, JsonUtils.toJson(parameters));
        jobData.put(JobParametersKeyConstants.CONNECTION_CONFIG, JobUtils.toJson(config));
        jobData.put(JobParametersKeyConstants.FLOW_INSTANCE_ID, exceptedTaskId + "");
        jobData.put(JobParametersKeyConstants.CURRENT_SCHEMA, config.getDefaultSchema());
        jobData.put(JobParametersKeyConstants.TASK_EXECUTION_TIMEOUT_MILLIS, 30 * 60 * 1000 + "");
        ObjectStorageConfiguration storageConfig = new ObjectStorageConfiguration();
        storageConfig.setCloudProvider(CloudProvider.NONE);

        return DefaultJobDefinition.builder().jobClass(DatabaseChangeTask.class)
                .jobType(TaskType.ASYNC.name())
                .jobParameters(jobData)
                .build();
    }

}