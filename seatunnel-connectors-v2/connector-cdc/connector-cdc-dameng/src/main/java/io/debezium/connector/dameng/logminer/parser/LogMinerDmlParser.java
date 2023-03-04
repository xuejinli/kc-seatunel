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

package io.debezium.connector.dameng.logminer.parser;

import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import lombok.NonNull;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.debezium.connector.dameng.logminer.parser.SQLSymbolChecker.NAMING_CHECKER;
import static io.debezium.connector.dameng.logminer.parser.SQLSymbolChecker.NUMBER_CHECKER;
import static io.debezium.connector.dameng.logminer.parser.SQLSymbolChecker.SKIP_CHECKER;

/**
 * A simple DML parser implementation specifically for Oracle LogMiner.
 *
 * <p>The syntax of each DML operation is restricted to the format generated by Oracle LogMiner. The
 * following are examples of each expected syntax:
 *
 * <pre>
 *     insert into "schema"."table"("C1","C2") values ('v1','v2');
 *     update "schema"."table" set "C1" = 'v1a', "C2" = 'v2a' where "C1" = 'v1' and "C2" = 'v2';
 *     delete from "schema"."table" where "C1" = 'v1' AND "C2" = 'v2';
 * </pre>
 *
 * Certain data types are not emitted as string literals, such as {@code DATE}/{@code TIME} and
 * {@code TIMESTAMP}/{@code DATETIME}.. For these data types, they're emitted as function calls. The
 * parser can detect this use case and will emit the values for such columns as the explicit
 * function call.
 *
 * <p>Lets take the following {@code UPDATE} statement:
 *
 * <pre>
 *     update "schema"."table"
 *        set "C1" = TO_TIMESTAMP('2020-02-02 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
 *      where "C1" = TO_TIMESTAMP('2020-02-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS');
 * </pre>
 *
 * The new value for {@code C1} would be {@code TIMESTAMP'2020-02-02 00:00:00'}. The old value for
 * {@code C1} would be {@code TIMESTAMP'2020-02-01 00:00:00'}.
 */
@SuppressWarnings("MagicNumber")
public class LogMinerDmlParser implements DmlParser {
    private static final String INSERT = "INSERT";
    private static final String INTO = "INTO";
    private static final String UPDATE = "UPDATE";
    private static final String DELETE = "DELETE";
    private static final String NULL_VALUE = "NULL";
    private static final String DATE = "DATE";
    private static final String TIME = "TIME";
    private static final String TIMESTAMP = "TIMESTAMP";
    private static final String DATETIME = "DATETIME";
    private static final char NAME_QUOTE = '"';
    private static final char VALUE_QUOTE = '\'';
    private static final char ESCAPE = '\\';
    private static final char SQL_END_OF = ';';
    private static final DateTimeFormatter DATE_TIME_FORMAT =
            new DateTimeFormatterBuilder()
                    .parseCaseInsensitive()
                    .append(DateTimeFormatter.ISO_LOCAL_DATE)
                    .appendLiteral(' ')
                    .append(DateTimeFormatter.ISO_LOCAL_TIME)
                    .toFormatter();

    @Override
    public LogMinerDmlEntry parse(@NonNull String sql, @NonNull Table table, String txId) {
        if (sql != null && sql.length() > 0) {
            switch (sql.charAt(0)) {
                case 'i':
                case 'I':
                    return parseInsert(sql, table, txId);
                case 'u':
                case 'U':
                    return parseUpdate(sql, table, txId);
                case 'd':
                case 'D':
                    return parseDelete(sql, table, txId);
                default:
            }
        }
        throw new DmlParserException("Unknown supported SQL '" + sql + "'");
    }

    @Override
    public LogMinerDmlEntry parseInsert(@NonNull String sql, @NonNull Table table, String txId) {
        SQLReader reader = new SQLReader(sql);
        checkArgument(
                reader.equalsIgnoreCaseAndMove(INSERT)
                        && reader.nextAndSkip(SKIP_CHECKER)
                        && reader.equalsIgnoreCaseAndMove(INTO)
                        && reader.nextAndSkip(SKIP_CHECKER),
                "SQL must start with 'INSERT INTO'");

        TableId tableId = readTableId(reader);
        List<String> columnNames = new ArrayList<>();
        reader.currentCheck('(', "Can't found '(' after table name");
        do {
            reader.nextAndSkip(SKIP_CHECKER);
            String columnName = this.readName(reader, "Column name is null");
            reader.nextAndSkip(SKIP_CHECKER);
            columnNames.add(columnName);
        } while (reader.current(','));
        reader.currentCheck(')', "Can't found ')' end columns");
        reader.nextAndSkip(SKIP_CHECKER);

        checkArgument(
                reader.equalsIgnoreCaseAndMove("values") || reader.equalsIgnoreCaseAndMove("value"),
                "Can't found 'values' or 'value'");
        checkArgument(
                reader.current('(') || reader.nextAndSkip(SKIP_CHECKER),
                "Can't found 'values' or 'value'");

        reader.currentCheck('(', "Can't found '(' after values");
        LinkedHashMap<String, Object> newColumnValues = new LinkedHashMap<>(columnNames.size());
        for (int i = 0; i < columnNames.size(); i++) {
            if (i != 0) {
                reader.currentCheck(',', "Can't found more values");
            }
            reader.nextAndSkip(SKIP_CHECKER);
            Object value = readValue(reader);
            newColumnValues.put(columnNames.get(i), value);
            reader.nextAndSkip(SKIP_CHECKER);
        }
        reader.currentCheck(')', "Can't found ')' end values");

        List<Column> debeziumTableColumns = table.columns();
        Object[] newValues = new Object[debeziumTableColumns.size()];
        for (int i = 0; i < debeziumTableColumns.size(); i++) {
            Column column = debeziumTableColumns.get(i);
            newValues[i] = newColumnValues.get(column.name());
        }
        return LogMinerDmlEntryImpl.forInsert(newValues)
                .setObjectOwner(tableId.schema())
                .setObjectName(tableId.table());
    }

    @Override
    public LogMinerDmlEntry parseUpdate(@NonNull String sql, @NonNull Table table, String txId) {
        SQLReader reader = new SQLReader(sql + SQL_END_OF);
        checkArgument(
                reader.equalsIgnoreCaseAndMove(UPDATE) && reader.nextAndSkip(SKIP_CHECKER),
                "SQL must start with 'UPDATE'");

        TableId tableId = readTableId(reader);
        checkArgument(
                reader.equalsIgnoreCaseAndMove("set") && reader.nextAndSkip(SKIP_CHECKER),
                "Can't found 'set' after table name");

        Map<String, Object> newColumnValues = new LinkedHashMap<>(table.columns().size());
        while (true) {
            String columnName = readName(reader, "Can't found column name");
            reader.nextAndSkip(SKIP_CHECKER);

            Object newValue = readConditionValue(reader);
            newColumnValues.put(columnName, newValue);

            reader.nextAndSkip(SKIP_CHECKER);
            if (reader.current(',')) {
                reader.nextAndSkip(SKIP_CHECKER);
                continue;
            }

            Map<String, Object> oldColumnValues = readWhereConditions(reader);

            List<Column> debeziumTableColumns = table.columns();
            Object[] oldValues = new Object[debeziumTableColumns.size()];
            Object[] newValues = new Object[debeziumTableColumns.size()];
            for (int i = 0; i < debeziumTableColumns.size(); i++) {
                String key = debeziumTableColumns.get(i).name();
                oldValues[i] = oldColumnValues.get(key);
                newValues[i] = oldColumnValues.get(key);
                if (newColumnValues.containsKey(key)) {
                    newValues[i] = newColumnValues.get(key);
                }
            }
            return LogMinerDmlEntryImpl.forUpdate(newValues, oldValues)
                    .setObjectOwner(tableId.schema())
                    .setObjectOwner(tableId.table());
        }
    }

    @Override
    public LogMinerDmlEntry parseDelete(@NonNull String sql, @NonNull Table table, String txId) {
        SQLReader reader = new SQLReader(sql + SQL_END_OF);
        checkArgument(
                reader.equalsIgnoreCaseAndMove(DELETE)
                        && reader.nextAndSkip(SKIP_CHECKER)
                        && reader.equalsIgnoreCaseAndMove("from")
                        && reader.nextAndSkip(SKIP_CHECKER),
                "SQL must start with 'DELETE FROM'");

        TableId tableId = readTableId(reader);

        Map<String, Object> oldColumnValues = readWhereConditions(reader);
        List<Column> debeziumTableColumns = table.columns();
        Object[] oldValues = new Object[debeziumTableColumns.size()];
        for (int i = 0; i < debeziumTableColumns.size(); i++) {
            Column column = debeziumTableColumns.get(i);
            oldValues[i] = oldColumnValues.get(column.name());
        }
        return LogMinerDmlEntryImpl.forDelete(oldValues)
                .setObjectOwner(tableId.schema())
                .setObjectName(tableId.table());
    }

    private static Map<String, Object> readWhereConditions(SQLReader reader) {
        checkArgument(reader.equalsIgnoreCaseAndMove("where"), "Can't found 'where'");

        Map<String, Object> oldColumnValues = new HashMap<>();
        for (int i = 0; reader.nextAndSkip(SKIP_CHECKER) && !reader.current(SQL_END_OF); i++) {
            if (i != 0) {
                checkArgument(reader.equalsIgnoreCaseAndMove("and"), "Condition not start 'and'");
                reader.nextAndSkip(SKIP_CHECKER);
            }
            String columnName = readName(reader, "Can't found column name");
            reader.nextAndSkip(SKIP_CHECKER);
            Object oldValue = readConditionValue(reader);
            oldColumnValues.put(columnName, oldValue);
        }
        return oldColumnValues;
    }

    private static Object readConditionValue(SQLReader reader) {
        switch (reader.getCurrent()) {
            case '=':
                reader.nextAndSkip(SKIP_CHECKER);
                return readValue(reader);
            case 'I':
            case 'i':
                if ('s' == reader.next() || 'S' == reader.getCurrent()) {
                    reader.nextAndSkip(SKIP_CHECKER);
                    return readValue(reader);
                }
                throw reader.exception("Not found expression: IS ... ");
            default:
                throw reader.exception("Not found condition");
        }
    }

    private static String readName(SQLReader reader, String errorMsg) {
        return reader.current(NAME_QUOTE)
                ? reader.loadInQuote(50, ESCAPE)
                : reader.loadIn(NAMING_CHECKER, errorMsg);
    }

    @SuppressWarnings("MagicNumber")
    private static Object readValue(SQLReader reader) {
        if (reader.current(VALUE_QUOTE)) {
            return reader.loadInQuote(50, ESCAPE);
        }
        if (NUMBER_CHECKER.contains(reader.getCurrent())) {
            return reader.loadIn(NUMBER_CHECKER, "Can't found number value");
        }

        String tmp = readName(reader, "Can't found function name");
        if (Boolean.TRUE.toString().equalsIgnoreCase(tmp)) {
            return true;
        }
        if (Boolean.FALSE.toString().equalsIgnoreCase(tmp)) {
            return false;
        }
        if (NULL_VALUE.equalsIgnoreCase(tmp)) {
            return null;
        }
        if (DATE.equalsIgnoreCase(tmp)) {
            reader.nextAndSkip(SKIP_CHECKER);
            String dateStr = (String) readValue(reader);
            return LocalDate.parse(dateStr, DateTimeFormatter.ISO_DATE);
        }
        if (TIME.equalsIgnoreCase(tmp)) {
            reader.nextAndSkip(SKIP_CHECKER);
            String timeStr = (String) readValue(reader);
            return LocalTime.parse(timeStr, DateTimeFormatter.ISO_TIME);
        }
        if (TIMESTAMP.equalsIgnoreCase(tmp)) {
            reader.nextAndSkip(SKIP_CHECKER);
            String timestampStr = (String) readValue(reader);
            return LocalDateTime.parse(timestampStr, DATE_TIME_FORMAT);
        }
        if (DATETIME.equalsIgnoreCase(tmp)) {
            reader.nextAndSkip(SKIP_CHECKER);
            String datetimeStr = (String) readValue(reader);
            return LocalDateTime.parse(datetimeStr, DATE_TIME_FORMAT);
        }
        if (reader.nextAndSkip(SKIP_CHECKER) && reader.current('(')) {
            return tmp + reader.loadInQuoteMulti(50, ')');
        }
        throw reader.exception("Value error '" + tmp + "'");
    }

    private static TableId readTableId(SQLReader reader) {
        String schemaName = readName(reader, "Table name is null");
        reader.nextAndSkip(SKIP_CHECKER);

        reader.currentCheck('.', "Table name is null");
        reader.nextAndSkip(SKIP_CHECKER);

        String tableName = readName(reader, "Table name is null");
        reader.nextAndSkip(SKIP_CHECKER);

        return new TableId(null, schemaName, tableName);
    }

    private static void checkArgument(boolean expression, String errorMessage) {
        if (!expression) {
            throw new IllegalArgumentException(String.valueOf(errorMessage));
        }
    }
}
