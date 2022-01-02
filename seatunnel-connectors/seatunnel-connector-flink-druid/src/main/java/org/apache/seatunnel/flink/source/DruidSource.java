/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.flink.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.config.Config;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.batch.FlinkBatchSource;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

public class DruidSource implements FlinkBatchSource<Row> {

    private Config config;

    private DruidInputFormat druidInputFormat;

    private static final String PATH = "path";
    private static final String SOURCE_FORMAT = "format.type";
    private static final String SCHEMA = "schema";

    private static final String JDBC_URL = "jdbc_url";
    private static final String DATASOURCE = "datasource";
    private static final String START_TIMESTAMP = "start_timestamp";
    private static final String END_TIMESTAMP = "end_timestamp";
    private static final String COLUMNS = "columns";

    private HashMap<String, TypeInformation> informationMapping = new HashMap<>();

    @Override
    public DataSet<Row> getData(FlinkEnvironment env) {
        return env.getBatchEnvironment().createInput(druidInputFormat);
    }

    @Override
    public void setConfig(Config config) {
        this.config = config;
    }

    @Override
    public Config getConfig() {
        return config;
    }

    @Override
    public CheckResult checkConfig() {
        return CheckConfigUtil.check(config, PATH, SOURCE_FORMAT, SCHEMA);
    }

    @Override
    public void prepare(FlinkEnvironment env) {
        String jdbcURL = config.getString(JDBC_URL);
        String datasource = config.getString(DATASOURCE);
        Long startTimestamp = config.hasPath(START_TIMESTAMP) ? config.getLong(START_TIMESTAMP) : null;
        Long endTimestamp = config.hasPath(END_TIMESTAMP) ? config.getLong(END_TIMESTAMP) : null;
        List<String> columns = config.hasPath(COLUMNS) ? config.getStringList(COLUMNS) : null;

        String sql = new DruidSql(datasource, startTimestamp, endTimestamp, columns).sql();

        this.druidInputFormat = DruidInputFormat.buildDruidInputFormat()
                .setDBUrl(jdbcURL)
                .setQuery(sql)
                .setRowTypeInfo(getRowTypeInfo(jdbcURL, datasource, columns))
                .finish();
    }

    private RowTypeInfo getRowTypeInfo(String jdbcURL, String datasource, Collection<String> userColumns) {
        HashMap<String, TypeInformation> map = new LinkedHashMap<>();

        try (Connection connection = DriverManager.getConnection(jdbcURL)) {
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet columns = metaData.getColumns(connection.getCatalog(), connection.getSchema(), datasource, "%");
            while (columns.next()) {
                String columnName = columns.getString("COLUMN_NAME");
                String dataTypeName = columns.getString("TYPE_NAME");
                if (userColumns == null || userColumns.contains(columnName)) {
                    map.put(columnName, informationMapping.get(dataTypeName));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        int size = map.size();
        if (userColumns != null && userColumns.size() > 0) {
            size = userColumns.size();
        } else {
            userColumns = map.keySet();
        }

        TypeInformation<?>[] typeInformation = new TypeInformation<?>[size];
        String[] names = new String[size];
        int i = 0;

        for (String field : userColumns) {
            typeInformation[i] = map.get(field);
            names[i] = field;
            i++;
        }
        return new RowTypeInfo(typeInformation, names);
    }
}
