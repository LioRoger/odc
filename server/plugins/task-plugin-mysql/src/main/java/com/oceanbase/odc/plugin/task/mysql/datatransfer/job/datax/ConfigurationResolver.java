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

package com.oceanbase.odc.plugin.task.mysql.datatransfer.job.datax;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.ConfigParser;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.google.common.collect.ImmutableMap;
import com.oceanbase.odc.core.shared.exception.UnsupportedException;
import com.oceanbase.odc.plugin.task.api.datatransfer.model.CsvColumnMapping;
import com.oceanbase.odc.plugin.task.api.datatransfer.model.CsvConfig;
import com.oceanbase.odc.plugin.task.api.datatransfer.model.DataTransferConfig;
import com.oceanbase.odc.plugin.task.api.datatransfer.model.DataTransferFormat;
import com.oceanbase.odc.plugin.task.api.datatransfer.model.ObjectResult;
import com.oceanbase.odc.plugin.task.mysql.datatransfer.common.Constants;
import com.oceanbase.odc.plugin.task.mysql.datatransfer.job.datax.model.JobConfiguration;
import com.oceanbase.odc.plugin.task.mysql.datatransfer.job.datax.model.JobContent;
import com.oceanbase.odc.plugin.task.mysql.datatransfer.job.datax.model.JobContent.Parameter;
import com.oceanbase.odc.plugin.task.mysql.datatransfer.job.datax.model.parameter.MySQLReaderPluginParameter;
import com.oceanbase.odc.plugin.task.mysql.datatransfer.job.datax.model.parameter.MySQLWriterPluginParameter;
import com.oceanbase.odc.plugin.task.mysql.datatransfer.job.datax.model.parameter.MySQLWriterPluginParameter.Connection;
import com.oceanbase.odc.plugin.task.mysql.datatransfer.job.datax.model.parameter.TxtPluginParameter.DataXCsvConfig;
import com.oceanbase.odc.plugin.task.mysql.datatransfer.job.datax.model.parameter.TxtReaderPluginParameter;
import com.oceanbase.odc.plugin.task.mysql.datatransfer.job.datax.model.parameter.TxtReaderPluginParameter.Column;
import com.oceanbase.odc.plugin.task.mysql.datatransfer.job.datax.model.parameter.TxtWriterPluginParameter;

/**
 * We hope to configure the job in the form of an Object instead of a config file. So this class
 * contains some code rewrites for the {@link ConfigParser} class to implement the process of custom
 * config loading.
 * 
 * @author liuyizhuo.lyz
 * @date 2023-09-24
 */
public class ConfigurationResolver {
    private static final String JAR_PATH;

    static {
        try {
            JAR_PATH =
                    new File(ConfigurationResolver.class.getProtectionDomain().getCodeSource().getLocation().toURI())
                            .getParentFile().getPath();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public static Configuration resolve(JobConfiguration jobConfig) {
        Configuration custom = Configuration.from(ImmutableMap.of("job", jobConfig));
        mergeCoreConfiguration(custom);
        mergePluginConfiguration(custom);
        return custom;
    }

    @SuppressWarnings("all")
    /**
     * <pre>
     * 
     * When importing data, user can configure column mapping relationships. OB-LOADER-DUMPER provides a
     * direct interface to enter the mapping. In datax, the way we configure the mapping relationship
     * is: 
     * 1. Specify the index and type of the columns to be read in the {@link TxtReaderPluginParameter#column}. 
     * 2. Specify the column names to be written in the {@link MySQLWriterPluginParameter#column}. The column 
     * names must be in strict order.
     *
     * </pre>
     */
    public static JobConfiguration buildJobConfigurationForImport(DataTransferConfig baseConfig, String jdbcUrl,
            ObjectResult object, URL resource) {
        if (baseConfig.getDataTransferFormat() == DataTransferFormat.SQL) {
            throw new UnsupportedException("SQL files should not be imported by DataX!");
        }

        JobConfiguration jobConfig = new JobConfiguration();
        JobContent jobContent = new JobContent();

        Long errorRecordLimit = baseConfig.isStopWhenError() ? 0L : null;
        jobConfig.getSetting().getErrorLimit().setRecord(errorRecordLimit);

        jobContent.setReader(createTxtReaderParameter(baseConfig, resource));
        jobContent.setWriter(createMySQLWriterParameter(baseConfig, jdbcUrl, object.getName()));
        jobConfig.setContent(new JobContent[] {jobContent});
        return jobConfig;
    }

    public static JobConfiguration buildJobConfigurationForExport(File workingDir, DataTransferConfig baseConfig,
            String jdbcUrl, String table, List<String> columns) {
        JobConfiguration jobConfig = new JobConfiguration();
        JobContent jobContent = new JobContent();

        Long errorRecordLimit = baseConfig.isStopWhenError() ? 0L : null;
        jobConfig.getSetting().getErrorLimit().setRecord(errorRecordLimit);

        jobContent.setReader(createMySQLReaderParameter(baseConfig, jdbcUrl, table));
        jobContent.setWriter(createTxtWriterParameter(workingDir, baseConfig, table, columns));
        jobConfig.setContent(new JobContent[] {jobContent});
        return jobConfig;
    }

    private static Parameter createTxtReaderParameter(DataTransferConfig baseConfig, URL input) {

        Parameter reader = new Parameter();
        TxtReaderPluginParameter pluginParameter = new TxtReaderPluginParameter();
        reader.setName(Constants.TXT_FILE_READER);
        reader.setParameter(pluginParameter);

        // common
        pluginParameter.setEncoding(baseConfig.getEncoding().getAlias());
        pluginParameter.setFileFormat("csv");
        // path
        pluginParameter.setPath(Collections.singletonList(input.getPath()));
        // column
        List<Object> column = Collections.singletonList("*");
        if (CollectionUtils.isNotEmpty(baseConfig.getCsvColumnMappings())) {
            column = baseConfig.getCsvColumnMappings().stream()
                    .map(mapping -> new Column(mapping.getSrcColumnPosition(), "string"))
                    .collect(Collectors.toList());
        }
        pluginParameter.setColumn(column);
        // csv config
        if (Objects.nonNull(baseConfig.getCsvConfig())) {
            pluginParameter.setCsvReaderConfig(getDataXCsvConfig(baseConfig));
            pluginParameter.setSkipHeader(baseConfig.getCsvConfig().isSkipHeader());
            pluginParameter.setNullFormat(baseConfig.getCsvConfig().isBlankToNull() ? "null" : "");
        }

        return reader;
    }

    private static Parameter createTxtWriterParameter(File workingDir, DataTransferConfig baseConfig, String table,
            List<String> columns) {
        Parameter writer = new Parameter();
        TxtWriterPluginParameter pluginParameter = new TxtWriterPluginParameter();
        writer.setName(Constants.TXT_FILE_WRITER);
        writer.setParameter(pluginParameter);

        // common
        pluginParameter.setEncoding(baseConfig.getEncoding().getAlias());
        pluginParameter.setFileFormat(baseConfig.getDataTransferFormat() == DataTransferFormat.SQL ? "sql" : "csv");
        pluginParameter.setLineDelimiter(getRealLineSeparator(baseConfig.getCsvConfig().getLineSeparator()));
        // path
        pluginParameter.setPath(Paths.get(workingDir.getPath(), "data", "TABLE").toString());
        pluginParameter.setFileName(table + baseConfig.getDataTransferFormat().getExtension());
        // header
        pluginParameter.setHeader(columns);
        pluginParameter.setTable(table);
        // sql config
        pluginParameter.setQuoteChar("`");
        if (Objects.nonNull(baseConfig.getBatchCommitNum())) {
            pluginParameter.setCommitSize(baseConfig.getBatchCommitNum());
        }
        // csv config
        if (Objects.nonNull(baseConfig.getCsvConfig())) {
            pluginParameter.setCsvWriterConfig(getDataXCsvConfig(baseConfig));
            pluginParameter.setSkipHeader(baseConfig.getCsvConfig().isSkipHeader());
            pluginParameter.setNullFormat(baseConfig.getCsvConfig().isBlankToNull() ? "null" : "");
        }

        return writer;
    }

    private static Parameter createMySQLReaderParameter(DataTransferConfig baseConfig, String url, String table) {
        Parameter reader = new Parameter();
        MySQLReaderPluginParameter pluginParameter = new MySQLReaderPluginParameter();
        reader.setName(Constants.MYSQL_READER);
        reader.setParameter(pluginParameter);

        // connection
        pluginParameter.setUsername(baseConfig.getConnectionInfo().getUserNameForConnect());
        pluginParameter.setPassword(baseConfig.getConnectionInfo().getPassword());
        MySQLReaderPluginParameter.Connection connection = new MySQLReaderPluginParameter.Connection(
                new String[] {url}, new String[] {table});
        pluginParameter.setConnection(Collections.singletonList(connection));
        // querySql
        if (Objects.nonNull(baseConfig.getQuerySql())) {
            pluginParameter.setQuerySql(Collections.singletonList(baseConfig.getQuerySql()));
        }

        return reader;
    }

    private static Parameter createMySQLWriterParameter(DataTransferConfig baseConfig, String url, String table) {
        Parameter writer = new Parameter();
        MySQLWriterPluginParameter pluginParameter = new MySQLWriterPluginParameter();
        writer.setName(Constants.MYSQL_WRITER);
        writer.setParameter(pluginParameter);

        pluginParameter.setWriteMode("insert");
        // connection
        pluginParameter.setUsername(baseConfig.getConnectionInfo().getUserNameForConnect());
        pluginParameter.setPassword(baseConfig.getConnectionInfo().getPassword());
        Connection connection = new Connection(url, new String[] {table});
        pluginParameter.setConnection(Collections.singletonList(connection));
        // preSql
        List<String> preSql = Arrays.asList(Constants.DISABLE_FK);
        if (baseConfig.isTruncateTableBeforeImport()) {
            preSql.add("TRUNCATE TABLE " + table);
        }
        pluginParameter.setPreSql(preSql);
        // postSql
        pluginParameter.setPostSql(Collections.singletonList(Constants.ENABLE_FK));
        // column
        List<String> column = Collections.singletonList("*");
        if (CollectionUtils.isNotEmpty(baseConfig.getCsvColumnMappings())) {
            column = baseConfig.getCsvColumnMappings().stream()
                    .sorted(Comparator.comparingInt(CsvColumnMapping::getSrcColumnPosition))
                    .map(CsvColumnMapping::getDestColumnName)
                    .collect(Collectors.toList());
        }
        pluginParameter.setColumn(column);

        return writer;
    }

    /**
     * load core.json from classpath:/datax/conf/
     */
    private static void mergeCoreConfiguration(Configuration custom) {
        custom.merge(
                Configuration.from(ConfigurationResolver.class.getResourceAsStream("/datax/conf/core.json")), false);
    }

    /**
     * load plugin.json from classpath:/datax/plugin/
     */
    public static void mergePluginConfiguration(Configuration custom) {

        String readerPluginName = custom.getString(CoreConstant.DATAX_JOB_CONTENT_READER_NAME);
        String writerPluginName = custom.getString(CoreConstant.DATAX_JOB_CONTENT_WRITER_NAME);
        String readerPluginResource = "/datax/plugin/reader/" + readerPluginName + "/plugin.json";
        custom.merge(parseSinglePluginConfig(readerPluginResource, "reader"), true);

        String writerPluginResource = "/datax/plugin/writer/" + writerPluginName + "/plugin.json";
        custom.merge(parseSinglePluginConfig(writerPluginResource, "writer"), true);

    }

    private static Configuration parseSinglePluginConfig(String resourceLocation, String type) {
        try {
            Configuration configuration =
                    Configuration.from(ConfigurationResolver.class.getResourceAsStream(resourceLocation));
            String pluginName = configuration.getString("name");

            configuration.set("path", JAR_PATH);
            configuration.set("loadType", "jarLoader");

            Configuration result = Configuration.newDefault();

            result.set(
                    String.format("plugin.%s.%s", type, pluginName),
                    configuration.getInternal());

            return result;
        } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to load datax-plugin[%s]", resourceLocation), e);
        }
    }

    private static DataXCsvConfig getDataXCsvConfig(DataTransferConfig baseConfig) {
        CsvConfig csvConfig = baseConfig.getCsvConfig();
        if (csvConfig == null) {
            csvConfig = new CsvConfig();
        }
        return DataXCsvConfig.builder()
                .textQualifier(csvConfig.getColumnDelimiter())
                .delimiter(csvConfig.getColumnSeparator())
                .recordDelimiter(getRealLineSeparator(csvConfig.getLineSeparator()))
                .build();
    }

    private static String getRealLineSeparator(String lineSeparator) {
        StringBuilder realLineSeparator = new StringBuilder();
        int length = lineSeparator.length();
        boolean transferFlag = false;
        for (int i = 0; i < length; i++) {
            char item = lineSeparator.charAt(i);
            if (item == '\\') {
                transferFlag = true;
                continue;
            }
            if (transferFlag) {
                if (item == 'n') {
                    realLineSeparator.append('\n');
                } else if (item == 'r') {
                    realLineSeparator.append('\r');
                }
                transferFlag = false;
            } else {
                realLineSeparator.append(item);
            }
        }
        return realLineSeparator.toString();
    }

}
