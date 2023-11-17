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

package org.apache.seatunnel.connectors.seatunnel.fake.config;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.List;
import java.util.Map;

public class FakeOption {

    public static final Option<List<Map<String, Object>>> TABLES_CONFIGS =
            Options.key("tables_configs")
                    .type(new TypeReference<List<Map<String, Object>>>() {})
                    .noDefaultValue()
                    .withDescription("The multiple table config list of fake source");

    public static final Option<List<Map<String, Object>>> ROWS =
            Options.key("rows")
                    .type(new TypeReference<List<Map<String, Object>>>() {})
                    .noDefaultValue()
                    .withDescription("The row list of fake data output per degree of parallelism");
    public static final Option<Integer> ROW_NUM =
            Options.key("row.num")
                    .intType()
                    .defaultValue(5)
                    .withDescription(
                            "The total number of data generated per degree of parallelism");
    public static final Option<Integer> SPLIT_NUM =
            Options.key("split.num")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "The number of splits generated by the enumerator for each degree of parallelism");
    public static final Option<Integer> SPLIT_READ_INTERVAL =
            Options.key("split.read-interval")
                    .intType()
                    .defaultValue(1)
                    .withDescription("The interval(mills) between two split reads in a reader");
    public static final Option<Integer> MAP_SIZE =
            Options.key("map.size")
                    .intType()
                    .defaultValue(5)
                    .withDescription("The size of map type that connector generated");
    public static final Option<Integer> ARRAY_SIZE =
            Options.key("array.size")
                    .intType()
                    .defaultValue(5)
                    .withDescription("The size of array type that connector generated");
    public static final Option<Integer> BYTES_LENGTH =
            Options.key("bytes.length")
                    .intType()
                    .defaultValue(5)
                    .withDescription("The length of bytes type that connector generated");
    public static final Option<Integer> STRING_LENGTH =
            Options.key("string.length")
                    .intType()
                    .defaultValue(5)
                    .withDescription("The length of string type that connector generated");

    public static final Option<List<String>> STRING_TEMPLATE =
            Options.key("string.template")
                    .listType()
                    .noDefaultValue()
                    .withDescription(
                            "The template list of string type that connector generated, if user configured it, connector will randomly select an item from the template list");

    public static final Option<List<Integer>> TINYINT_TEMPLATE =
            Options.key("tinyint.template")
                    .listType(Integer.class)
                    .noDefaultValue()
                    .withDescription(
                            "The template list of tinyint type, if user configured it, connector will randomly select an item from the template list");

    public static final Option<List<Integer>> SMALLINT_TEMPLATE =
            Options.key("smallint.template")
                    .listType(Integer.class)
                    .noDefaultValue()
                    .withDescription(
                            "The template list of smallint type, if user configured it, connector will randomly select an item from the template list");

    public static final Option<List<Integer>> INT_TEMPLATE =
            Options.key("int.template")
                    .listType(Integer.class)
                    .noDefaultValue()
                    .withDescription(
                            "The template list of int type, if user configured it, connector will randomly select an item from the template list");

    public static final Option<List<Long>> BIGINT_TEMPLATE =
            Options.key("bigint.template")
                    .listType(Long.class)
                    .noDefaultValue()
                    .withDescription(
                            "The template list of bigint type, if user configured it, connector will randomly select an item from the template list");

    public static final Option<List<Double>> FLOAT_TEMPLATE =
            Options.key("float.template")
                    .listType(Double.class)
                    .noDefaultValue()
                    .withDescription(
                            "The template list of float type, if user configured it, connector will randomly select an item from the template list");

    public static final Option<List<Double>> DOUBLE_TEMPLATE =
            Options.key("double.template")
                    .listType(Double.class)
                    .noDefaultValue()
                    .withDescription(
                            "The template list of double type, if user configured it, connector will randomly select an item from the template list");

    public static final Option<List<Integer>> DATE_YEAR_TEMPLATE =
            Options.key("date.year.template")
                    .listType(Integer.class)
                    .noDefaultValue()
                    .withDescription(
                            "The template list of year of date like 'yyyy', if user configured it, connector will randomly select an item from the template list");

    public static final Option<List<Integer>> DATE_MONTH_TEMPLATE =
            Options.key("date.month.template")
                    .listType(Integer.class)
                    .noDefaultValue()
                    .withDescription(
                            "The template list of month of date like 'MM', if user configured it, connector will randomly select an item from the template list");

    public static final Option<List<Integer>> DATE_DAY_TEMPLATE =
            Options.key("date.day.template")
                    .listType(Integer.class)
                    .noDefaultValue()
                    .withDescription(
                            "The template list of day of date like 'dd', if user configured it, connector will randomly select an item from the template list");

    public static final Option<List<Integer>> TIME_HOUR_TEMPLATE =
            Options.key("time.hour.template")
                    .listType(Integer.class)
                    .noDefaultValue()
                    .withDescription(
                            "The template list of hour of time like 'HH', if user configured it, connector will randomly select an item from the template list");

    public static final Option<List<Integer>> TIME_MINUTE_TEMPLATE =
            Options.key("time.minute.template")
                    .listType(Integer.class)
                    .noDefaultValue()
                    .withDescription(
                            "The template list of minute of time like 'mm', if user configured it, connector will randomly select an item from the template list");

    public static final Option<List<Integer>> TIME_SECOND_TEMPLATE =
            Options.key("time.second.template")
                    .listType(Integer.class)
                    .noDefaultValue()
                    .withDescription(
                            "The template list of second of time like 'ss', if user configured it, connector will randomly select an item from the template list");

    public static final Option<Integer> TINYINT_MIN =
            Options.key("tinyint.min")
                    .intType()
                    .defaultValue(0)
                    .withDescription("The min value of tinyint type data");

    public static final Option<Integer> TINYINT_MAX =
            Options.key("tinyint.max")
                    .intType()
                    .defaultValue((int) Byte.MAX_VALUE)
                    .withDescription("The min value of tinyint type data");

    public static final Option<Integer> SMALLINT_MIN =
            Options.key("smallint.min")
                    .intType()
                    .defaultValue(0)
                    .withDescription("The min value of smallint type data");

    public static final Option<Integer> SMALLINT_MAX =
            Options.key("smallint.max")
                    .intType()
                    .defaultValue((int) Short.MAX_VALUE)
                    .withDescription("The max value of smallint type data");

    public static final Option<Integer> INT_MIN =
            Options.key("int.min")
                    .intType()
                    .defaultValue(0)
                    .withDescription("The min value of int type data");

    public static final Option<Integer> INT_MAX =
            Options.key("int.max")
                    .intType()
                    .defaultValue(Integer.MAX_VALUE)
                    .withDescription("The max value of int type data");

    public static final Option<Long> BIGINT_MIN =
            Options.key("bigint.min")
                    .longType()
                    .defaultValue(0L)
                    .withDescription("The min value of bigint type data");

    public static final Option<Long> BIGINT_MAX =
            Options.key("bigint.max")
                    .longType()
                    .defaultValue(Long.MAX_VALUE)
                    .withDescription("The max value of bigint type data");

    public static final Option<Float> FLOAT_MIN =
            Options.key("float.min")
                    .floatType()
                    .defaultValue(0F)
                    .withDescription("The min value of float type data");

    public static final Option<Float> FLOAT_MAX =
            Options.key("float.max")
                    .floatType()
                    .defaultValue(Float.MAX_VALUE)
                    .withDescription("The max value of float type data");

    public static final Option<Double> DOUBLE_MIN =
            Options.key("double.min")
                    .doubleType()
                    .defaultValue(0D)
                    .withDescription("The min value of double type data");

    public static final Option<Double> DOUBLE_MAX =
            Options.key("double.max")
                    .doubleType()
                    .defaultValue(Double.MAX_VALUE)
                    .withDescription("The max value of double type data");

    public static final Option<FakeMode> STRING_FAKE_MODE =
            Options.key("string.fake.mode")
                    .enumType(FakeMode.class)
                    .defaultValue(FakeMode.RANGE)
                    .withDescription("The fake mode of generating string data");

    public static final Option<FakeMode> TINYINT_FAKE_MODE =
            Options.key("tinyint.fake.mode")
                    .enumType(FakeMode.class)
                    .defaultValue(FakeMode.RANGE)
                    .withDescription("The fake mode of generating tinyint data");

    public static final Option<FakeMode> SMALLINT_FAKE_MODE =
            Options.key("smallint.fake.mode")
                    .enumType(FakeMode.class)
                    .defaultValue(FakeMode.RANGE)
                    .withDescription("The fake mode of generating smallint data");

    public static final Option<FakeMode> INT_FAKE_MODE =
            Options.key("int.fake.mode")
                    .enumType(FakeMode.class)
                    .defaultValue(FakeMode.RANGE)
                    .withDescription("The fake mode of generating int data");

    public static final Option<FakeMode> BIGINT_FAKE_MODE =
            Options.key("bigint.fake.mode")
                    .enumType(FakeMode.class)
                    .defaultValue(FakeMode.RANGE)
                    .withDescription("The fake mode of generating bigint data");

    public static final Option<FakeMode> FLOAT_FAKE_MODE =
            Options.key("float.fake.mode")
                    .enumType(FakeMode.class)
                    .defaultValue(FakeMode.RANGE)
                    .withDescription("The fake mode of generating float data");

    public static final Option<FakeMode> DOUBLE_FAKE_MODE =
            Options.key("double.fake.mode")
                    .enumType(FakeMode.class)
                    .defaultValue(FakeMode.RANGE)
                    .withDescription("The fake mode of generating double data");

    public enum FakeMode {
        RANGE,
        TEMPLATE;

        public static FakeMode parse(String s) {
            return FakeMode.valueOf(s.toUpperCase());
        }
    }
}
