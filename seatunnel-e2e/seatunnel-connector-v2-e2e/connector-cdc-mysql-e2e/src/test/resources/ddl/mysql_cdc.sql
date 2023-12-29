--
-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--

-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE:  inventory
-- ----------------------------------------------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS `mysql_cdc`;

use mysql_cdc;
-- Create a mysql data source table
CREATE TABLE mysql_cdc_e2e_source_table
(
    `id`                   int       NOT NULL AUTO_INCREMENT,
    `f_binary`             binary(64)                     DEFAULT NULL,
    `f_blob`               blob,
    `f_long_varbinary`     mediumblob,
    `f_longblob`           longblob,
    `f_tinyblob`           tinyblob,
    `f_varbinary`          varbinary(100)                 DEFAULT NULL,
    `f_smallint`           smallint                       DEFAULT NULL,
    `f_smallint_unsigned`  smallint unsigned              DEFAULT NULL,
    `f_mediumint`          mediumint                      DEFAULT NULL,
    `f_mediumint_unsigned` mediumint unsigned             DEFAULT NULL,
    `f_int`                int                            DEFAULT NULL,
    `f_int_unsigned`       int unsigned                   DEFAULT NULL,
    `f_integer`            int                            DEFAULT NULL,
    `f_integer_unsigned`   int unsigned                   DEFAULT NULL,
    `f_bigint`             bigint                         DEFAULT NULL,
    `f_bigint_unsigned`    bigint unsigned                DEFAULT NULL,
    `f_numeric`            decimal(10, 0)                 DEFAULT NULL,
    `f_decimal`            decimal(10, 0)                 DEFAULT NULL,
    `f_float`              float                          DEFAULT NULL,
    `f_double`             double                         DEFAULT NULL,
    `f_double_precision`   double                         DEFAULT NULL,
    `f_longtext`           longtext,
    `f_mediumtext`         mediumtext,
    `f_text`               text,
    `f_tinytext`           tinytext,
    `f_varchar`            varchar(100)                   DEFAULT NULL,
    `f_date`               date                           DEFAULT NULL,
    `f_datetime`           datetime                       DEFAULT NULL,
    `f_timestamp`          timestamp NULL                 DEFAULT NULL,
    `f_bit1`               bit(1)                         DEFAULT NULL,
    `f_bit64`              bit(64)                        DEFAULT NULL,
    `f_char`               char(1)                        DEFAULT NULL,
    `f_enum`               enum ('enum1','enum2','enum3') DEFAULT NULL,
    `f_mediumblob`         mediumblob,
    `f_long_varchar`       mediumtext,
    `f_real`               double                         DEFAULT NULL,
    `f_time`               time                           DEFAULT NULL,
    `f_tinyint`            tinyint                        DEFAULT NULL,
    `f_tinyint_unsigned`   tinyint unsigned               DEFAULT NULL,
    `f_json`               json                           DEFAULT NULL,
    `f_year`               year                           DEFAULT NULL,
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  AUTO_INCREMENT = 2
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci;

CREATE TABLE mysql_cdc_e2e_source_table2
(
    `id`                   int       NOT NULL AUTO_INCREMENT,
    `f_binary`             binary(64)                     DEFAULT NULL,
    `f_blob`               blob,
    `f_long_varbinary`     mediumblob,
    `f_longblob`           longblob,
    `f_tinyblob`           tinyblob,
    `f_varbinary`          varbinary(100)                 DEFAULT NULL,
    `f_smallint`           smallint                       DEFAULT NULL,
    `f_smallint_unsigned`  smallint unsigned              DEFAULT NULL,
    `f_mediumint`          mediumint                      DEFAULT NULL,
    `f_mediumint_unsigned` mediumint unsigned             DEFAULT NULL,
    `f_int`                int                            DEFAULT NULL,
    `f_int_unsigned`       int unsigned                   DEFAULT NULL,
    `f_integer`            int                            DEFAULT NULL,
    `f_integer_unsigned`   int unsigned                   DEFAULT NULL,
    `f_bigint`             bigint                         DEFAULT NULL,
    `f_bigint_unsigned`    bigint unsigned                DEFAULT NULL,
    `f_numeric`            decimal(10, 0)                 DEFAULT NULL,
    `f_decimal`            decimal(10, 0)                 DEFAULT NULL,
    `f_float`              float                          DEFAULT NULL,
    `f_double`             double                         DEFAULT NULL,
    `f_double_precision`   double                         DEFAULT NULL,
    `f_longtext`           longtext,
    `f_mediumtext`         mediumtext,
    `f_text`               text,
    `f_tinytext`           tinytext,
    `f_varchar`            varchar(100)                   DEFAULT NULL,
    `f_date`               date                           DEFAULT NULL,
    `f_datetime`           datetime                       DEFAULT NULL,
    `f_timestamp`          timestamp NULL                 DEFAULT NULL,
    `f_bit1`               bit(1)                         DEFAULT NULL,
    `f_bit64`              bit(64)                        DEFAULT NULL,
    `f_char`               char(1)                        DEFAULT NULL,
    `f_enum`               enum ('enum1','enum2','enum3') DEFAULT NULL,
    `f_mediumblob`         mediumblob,
    `f_long_varchar`       mediumtext,
    `f_real`               double                         DEFAULT NULL,
    `f_time`               time                           DEFAULT NULL,
    `f_tinyint`            tinyint                        DEFAULT NULL,
    `f_tinyint_unsigned`   tinyint unsigned               DEFAULT NULL,
    `f_json`               json                           DEFAULT NULL,
    `f_year`               year                           DEFAULT NULL,
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  AUTO_INCREMENT = 2
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci;

CREATE TABLE mysql_cdc_e2e_source_table_no_primary_key
(
    `id`                   int                            NOT NULL,
    `f_binary`             binary(64)                     DEFAULT NULL,
    `f_blob`               blob,
    `f_long_varbinary`     mediumblob,
    `f_longblob`           longblob,
    `f_tinyblob`           tinyblob,
    `f_varbinary`          varbinary(100)                 DEFAULT NULL,
    `f_smallint`           smallint                       DEFAULT NULL,
    `f_smallint_unsigned`  smallint unsigned              DEFAULT NULL,
    `f_mediumint`          mediumint                      DEFAULT NULL,
    `f_mediumint_unsigned` mediumint unsigned             DEFAULT NULL,
    `f_int`                int                            DEFAULT NULL,
    `f_int_unsigned`       int unsigned                   DEFAULT NULL,
    `f_integer`            int                            DEFAULT NULL,
    `f_integer_unsigned`   int unsigned                   DEFAULT NULL,
    `f_bigint`             bigint                         DEFAULT NULL,
    `f_bigint_unsigned`    bigint unsigned                DEFAULT NULL,
    `f_numeric`            decimal(10, 0)                 DEFAULT NULL,
    `f_decimal`            decimal(10, 0)                 DEFAULT NULL,
    `f_float`              float                          DEFAULT NULL,
    `f_double`             double                         DEFAULT NULL,
    `f_double_precision`   double                         DEFAULT NULL,
    `f_longtext`           longtext,
    `f_mediumtext`         mediumtext,
    `f_text`               text,
    `f_tinytext`           tinytext,
    `f_varchar`            varchar(100)                   DEFAULT NULL,
    `f_date`               date                           DEFAULT NULL,
    `f_datetime`           datetime                       DEFAULT NULL,
    `f_timestamp`          timestamp NULL                 DEFAULT NULL,
    `f_bit1`               bit(1)                         DEFAULT NULL,
    `f_bit64`              bit(64)                        DEFAULT NULL,
    `f_char`               char(1)                        DEFAULT NULL,
    `f_enum`               enum ('enum1','enum2','enum3') DEFAULT NULL,
    `f_mediumblob`         mediumblob,
    `f_long_varchar`       mediumtext,
    `f_real`               double                         DEFAULT NULL,
    `f_time`               time                           DEFAULT NULL,
    `f_tinyint`            tinyint                        DEFAULT NULL,
    `f_tinyint_unsigned`   tinyint unsigned               DEFAULT NULL,
    `f_json`               json                           DEFAULT NULL,
    `f_year`               year                           DEFAULT NULL
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci;

CREATE TABLE mysql_cdc_e2e_sink_table
(
    `id`                   int       NOT NULL AUTO_INCREMENT,
    `f_binary`             binary(64)                     DEFAULT NULL,
    `f_blob`               blob,
    `f_long_varbinary`     mediumblob,
    `f_longblob`           longblob,
    `f_tinyblob`           tinyblob,
    `f_varbinary`          varbinary(100)                 DEFAULT NULL,
    `f_smallint`           smallint                       DEFAULT NULL,
    `f_smallint_unsigned`  smallint unsigned              DEFAULT NULL,
    `f_mediumint`          mediumint                      DEFAULT NULL,
    `f_mediumint_unsigned` mediumint unsigned             DEFAULT NULL,
    `f_int`                int                            DEFAULT NULL,
    `f_int_unsigned`       int unsigned                   DEFAULT NULL,
    `f_integer`            int                            DEFAULT NULL,
    `f_integer_unsigned`   int unsigned                   DEFAULT NULL,
    `f_bigint`             bigint                         DEFAULT NULL,
    `f_bigint_unsigned`    bigint unsigned                DEFAULT NULL,
    `f_numeric`            decimal(10, 0)                 DEFAULT NULL,
    `f_decimal`            decimal(10, 0)                 DEFAULT NULL,
    `f_float`              float                          DEFAULT NULL,
    `f_double`             double                         DEFAULT NULL,
    `f_double_precision`   double                         DEFAULT NULL,
    `f_longtext`           longtext,
    `f_mediumtext`         mediumtext,
    `f_text`               text,
    `f_tinytext`           tinytext,
    `f_varchar`            varchar(100)                   DEFAULT NULL,
    `f_date`               date                           DEFAULT NULL,
    `f_datetime`           datetime                       DEFAULT NULL,
    `f_timestamp`          timestamp NULL                 DEFAULT NULL,
    `f_bit1`               bit(1)                         DEFAULT NULL,
    `f_bit64`              bit(64)                        DEFAULT NULL,
    `f_char`               char(1)                        DEFAULT NULL,
    `f_enum`               enum ('enum1','enum2','enum3') DEFAULT NULL,
    `f_mediumblob`         mediumblob,
    `f_long_varchar`       mediumtext,
    `f_real`               double                         DEFAULT NULL,
    `f_time`               time                           DEFAULT NULL,
    `f_tinyint`            tinyint                        DEFAULT NULL,
    `f_tinyint_unsigned`   tinyint unsigned               DEFAULT NULL,
    `f_json`               json                           DEFAULT NULL,
    `f_year`               int                           DEFAULT NULL,
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  AUTO_INCREMENT = 2
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci;

truncate table mysql_cdc_e2e_source_table;
truncate table mysql_cdc_e2e_source_table2;
truncate table mysql_cdc_e2e_source_table_no_primary_key;
truncate table mysql_cdc_e2e_sink_table;

INSERT INTO mysql_cdc_e2e_source_table ( id, f_binary, f_blob, f_long_varbinary, f_longblob, f_tinyblob, f_varbinary, f_smallint,
                                         f_smallint_unsigned, f_mediumint, f_mediumint_unsigned, f_int, f_int_unsigned, f_integer,
                                         f_integer_unsigned, f_bigint, f_bigint_unsigned, f_numeric, f_decimal, f_float, f_double,
                                         f_double_precision, f_longtext, f_mediumtext, f_text, f_tinytext, f_varchar, f_date, f_datetime,
                                         f_timestamp, f_bit1, f_bit64, f_char, f_enum, f_mediumblob, f_long_varchar, f_real, f_time,
                                         f_tinyint, f_tinyint_unsigned, f_json, f_year )
VALUES ( 1, 0x61626374000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000,
         0x68656C6C6F, 0x18000000789C0BC9C82C5600A244859CFCBC7485B2C4A2A4CCBCC4A24A00697308D4, NULL,
         0x74696E79626C6F62, 0x48656C6C6F20776F726C64, 12345, 54321, 123456, 654321, 1234567, 7654321, 1234567, 7654321,
         123456789, 987654321, 123, 789, 12.34, 56.78, 90.12, 'This is a long text field', 'This is a medium text field',
         'This is a text field', 'This is a tiny text field', 'This is a varchar field', '2022-04-27', '2022-04-27 14:30:00',
         '2023-04-27 11:08:40', 1, b'0101010101010101010101010101010101010101010101010101010101010101', 'C', 'enum2',
         0x1B000000789C0BC9C82C5600A24485DCD494CCD25C85A49CFC2485B4CCD49C140083FF099A, 'This is a long varchar field',
         12.345, '14:30:00', -128, 255, '{ "key": "value" }', 2022 ),
       ( 2, 0x61626374000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000,
         0x68656C6C6F, 0x18000000789C0BC9C82C5600A244859CFCBC7485B2C4A2A4CCBCC4A24A00697308D4, NULL, 0x74696E79626C6F62,
         0x48656C6C6F20776F726C64, 12345, 54321, 123456, 654321, 1234567, 7654321, 1234567, 7654321, 123456789, 987654321,
         123, 789, 12.34, 56.78, 90.12, 'This is a long text field', 'This is a medium text field', 'This is a text field',
         'This is a tiny text field', 'This is a varchar field', '2022-04-27', '2022-04-27 14:30:00', '2023-04-27 11:08:40',
         1, b'0101010101010101010101010101010101010101010101010101010101010101', 'C', 'enum2',
         0x1B000000789C0BC9C82C5600A24485DCD494CCD25C85A49CFC2485B4CCD49C140083FF099A, 'This is a long varchar field',
         112.345, '14:30:00', -128, 22, '{ "key": "value" }', 2013 ),
       ( 3, 0x61626374000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000,
         0x68656C6C6F, 0x18000000789C0BC9C82C5600A244859CFCBC7485B2C4A2A4CCBCC4A24A00697308D4, NULL, 0x74696E79626C6F62,
         0x48656C6C6F20776F726C64, 12345, 54321, 123456, 654321, 1234567, 7654321, 1234567, 7654321, 123456789, 987654321, 123,
         789, 12.34, 56.78, 90.12, 'This is a long text field', 'This is a medium text field', 'This is a text field',
         'This is a tiny text field', 'This is a varchar field', '2022-04-27', '2022-04-27 14:30:00', '2023-04-27 11:08:40',
         1, b'0101010101010101010101010101010101010101010101010101010101010101', 'C', 'enum2',
         0x1B000000789C0BC9C82C5600A24485DCD494CCD25C85A49CFC2485B4CCD49C140083FF099A, 'This is a long varchar field', 112.345,
         '14:30:00', -128, 22, '{ "key": "value" }', 2021 );

INSERT INTO mysql_cdc_e2e_source_table2 ( id, f_binary, f_blob, f_long_varbinary, f_longblob, f_tinyblob, f_varbinary, f_smallint,
                                         f_smallint_unsigned, f_mediumint, f_mediumint_unsigned, f_int, f_int_unsigned, f_integer,
                                         f_integer_unsigned, f_bigint, f_bigint_unsigned, f_numeric, f_decimal, f_float, f_double,
                                         f_double_precision, f_longtext, f_mediumtext, f_text, f_tinytext, f_varchar, f_date, f_datetime,
                                         f_timestamp, f_bit1, f_bit64, f_char, f_enum, f_mediumblob, f_long_varchar, f_real, f_time,
                                         f_tinyint, f_tinyint_unsigned, f_json, f_year )
VALUES ( 1, 0x61626374000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000,
         0x68656C6C6F, 0x18000000789C0BC9C82C5600A244859CFCBC7485B2C4A2A4CCBCC4A24A00697308D4, NULL,
         0x74696E79626C6F62, 0x48656C6C6F20776F726C64, 12345, 54321, 123456, 654321, 1234567, 7654321, 1234567, 7654321,
         123456789, 987654321, 123, 789, 12.34, 56.78, 90.12, 'This is a long text field', 'This is a medium text field',
         'This is a text field', 'This is a tiny text field', 'This is a varchar field', '2022-04-27', '2022-04-27 14:30:00',
         '2023-04-27 11:08:40', 1, b'0101010101010101010101010101010101010101010101010101010101010101', 'C', 'enum2',
         0x1B000000789C0BC9C82C5600A24485DCD494CCD25C85A49CFC2485B4CCD49C140083FF099A, 'This is a long varchar field',
         12.345, '14:30:00', -128, 255, '{ "key": "value" }', 2022 ),
       ( 2, 0x61626374000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000,
         0x68656C6C6F, 0x18000000789C0BC9C82C5600A244859CFCBC7485B2C4A2A4CCBCC4A24A00697308D4, NULL, 0x74696E79626C6F62,
         0x48656C6C6F20776F726C64, 12345, 54321, 123456, 654321, 1234567, 7654321, 1234567, 7654321, 123456789, 987654321,
         123, 789, 12.34, 56.78, 90.12, 'This is a long text field', 'This is a medium text field', 'This is a text field',
         'This is a tiny text field', 'This is a varchar field', '2022-04-27', '2022-04-27 14:30:00', '2023-04-27 11:08:40',
         1, b'0101010101010101010101010101010101010101010101010101010101010101', 'C', 'enum2',
         0x1B000000789C0BC9C82C5600A24485DCD494CCD25C85A49CFC2485B4CCD49C140083FF099A, 'This is a long varchar field',
         112.345, '14:30:00', -128, 22, '{ "key": "value" }', 2013 ),
       ( 3, 0x61626374000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000,
         0x68656C6C6F, 0x18000000789C0BC9C82C5600A244859CFCBC7485B2C4A2A4CCBCC4A24A00697308D4, NULL, 0x74696E79626C6F62,
         0x48656C6C6F20776F726C64, 12345, 54321, 123456, 654321, 1234567, 7654321, 1234567, 7654321, 123456789, 987654321, 123,
         789, 12.34, 56.78, 90.12, 'This is a long text field', 'This is a medium text field', 'This is a text field',
         'This is a tiny text field', 'This is a varchar field', '2022-04-27', '2022-04-27 14:30:00', '2023-04-27 11:08:40',
         1, b'0101010101010101010101010101010101010101010101010101010101010101', 'C', 'enum2',
         0x1B000000789C0BC9C82C5600A24485DCD494CCD25C85A49CFC2485B4CCD49C140083FF099A, 'This is a long varchar field', 112.345,
         '14:30:00', -128, 22, '{ "key": "value" }', 2021 );

INSERT INTO mysql_cdc_e2e_source_table_no_primary_key ( id, f_binary, f_blob, f_long_varbinary, f_longblob, f_tinyblob, f_varbinary, f_smallint,
                                          f_smallint_unsigned, f_mediumint, f_mediumint_unsigned, f_int, f_int_unsigned, f_integer,
                                          f_integer_unsigned, f_bigint, f_bigint_unsigned, f_numeric, f_decimal, f_float, f_double,
                                          f_double_precision, f_longtext, f_mediumtext, f_text, f_tinytext, f_varchar, f_date, f_datetime,
                                          f_timestamp, f_bit1, f_bit64, f_char, f_enum, f_mediumblob, f_long_varchar, f_real, f_time,
                                          f_tinyint, f_tinyint_unsigned, f_json, f_year )
VALUES ( 1, 0x61626374000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000,
         0x68656C6C6F, 0x18000000789C0BC9C82C5600A244859CFCBC7485B2C4A2A4CCBCC4A24A00697308D4, NULL,
         0x74696E79626C6F62, 0x48656C6C6F20776F726C64, 12345, 54321, 123456, 654321, 1234567, 7654321, 1234567, 7654321,
         123456789, 987654321, 123, 789, 12.34, 56.78, 90.12, 'This is a long text field', 'This is a medium text field',
         'This is a text field', 'This is a tiny text field', 'This is a varchar field', '2022-04-27', '2022-04-27 14:30:00',
         '2023-04-27 11:08:40', 1, b'0101010101010101010101010101010101010101010101010101010101010101', 'C', 'enum2',
         0x1B000000789C0BC9C82C5600A24485DCD494CCD25C85A49CFC2485B4CCD49C140083FF099A, 'This is a long varchar field',
         12.345, '14:30:00', -128, 255, '{ "key": "value" }', 2022 ),
       ( 2, 0x61626374000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000,
         0x68656C6C6F, 0x18000000789C0BC9C82C5600A244859CFCBC7485B2C4A2A4CCBCC4A24A00697308D4, NULL, 0x74696E79626C6F62,
         0x48656C6C6F20776F726C64, 12345, 54321, 123456, 654321, 1234567, 7654321, 1234567, 7654321, 123456789, 987654321,
         123, 789, 12.34, 56.78, 90.12, 'This is a long text field', 'This is a medium text field', 'This is a text field',
         'This is a tiny text field', 'This is a varchar field', '2022-04-27', '2022-04-27 14:30:00', '2023-04-27 11:08:40',
         1, b'0101010101010101010101010101010101010101010101010101010101010101', 'C', 'enum2',
         0x1B000000789C0BC9C82C5600A24485DCD494CCD25C85A49CFC2485B4CCD49C140083FF099A, 'This is a long varchar field',
         112.345, '14:30:00', -128, 22, '{ "key": "value" }', 2013 ),
       ( 3, 0x61626374000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000,
         0x68656C6C6F, 0x18000000789C0BC9C82C5600A244859CFCBC7485B2C4A2A4CCBCC4A24A00697308D4, NULL, 0x74696E79626C6F62,
         0x48656C6C6F20776F726C64, 12345, 54321, 123456, 654321, 1234567, 7654321, 1234567, 7654321, 123456789, 987654321, 123,
         789, 12.34, 56.78, 90.12, 'This is a long text field', 'This is a medium text field', 'This is a text field',
         'This is a tiny text field', 'This is a varchar field', '2022-04-27', '2022-04-27 14:30:00', '2023-04-27 11:08:40',
         1, b'0101010101010101010101010101010101010101010101010101010101010101', 'C', 'enum2',
         0x1B000000789C0BC9C82C5600A24485DCD494CCD25C85A49CFC2485B4CCD49C140083FF099A, 'This is a long varchar field', 112.345,
         '14:30:00', -128, 22, '{ "key": "value" }', 2021 );

CREATE DATABASE IF NOT EXISTS `mysql_cdc2`;

use mysql_cdc2;
-- Create a mysql data source table
CREATE TABLE mysql_cdc_e2e_source_table
(
    `id`                   int       NOT NULL AUTO_INCREMENT,
    `f_binary`             binary(64)                     DEFAULT NULL,
    `f_blob`               blob,
    `f_long_varbinary`     mediumblob,
    `f_longblob`           longblob,
    `f_tinyblob`           tinyblob,
    `f_varbinary`          varbinary(100)                 DEFAULT NULL,
    `f_smallint`           smallint                       DEFAULT NULL,
    `f_smallint_unsigned`  smallint unsigned              DEFAULT NULL,
    `f_mediumint`          mediumint                      DEFAULT NULL,
    `f_mediumint_unsigned` mediumint unsigned             DEFAULT NULL,
    `f_int`                int                            DEFAULT NULL,
    `f_int_unsigned`       int unsigned                   DEFAULT NULL,
    `f_integer`            int                            DEFAULT NULL,
    `f_integer_unsigned`   int unsigned                   DEFAULT NULL,
    `f_bigint`             bigint                         DEFAULT NULL,
    `f_bigint_unsigned`    bigint unsigned                DEFAULT NULL,
    `f_numeric`            decimal(10, 0)                 DEFAULT NULL,
    `f_decimal`            decimal(10, 0)                 DEFAULT NULL,
    `f_float`              float                          DEFAULT NULL,
    `f_double`             double                         DEFAULT NULL,
    `f_double_precision`   double                         DEFAULT NULL,
    `f_longtext`           longtext,
    `f_mediumtext`         mediumtext,
    `f_text`               text,
    `f_tinytext`           tinytext,
    `f_varchar`            varchar(100)                   DEFAULT NULL,
    `f_date`               date                           DEFAULT NULL,
    `f_datetime`           datetime                       DEFAULT NULL,
    `f_timestamp`          timestamp NULL                 DEFAULT NULL,
    `f_bit1`               bit(1)                         DEFAULT NULL,
    `f_bit64`              bit(64)                        DEFAULT NULL,
    `f_char`               char(1)                        DEFAULT NULL,
    `f_enum`               enum ('enum1','enum2','enum3') DEFAULT NULL,
    `f_mediumblob`         mediumblob,
    `f_long_varchar`       mediumtext,
    `f_real`               double                         DEFAULT NULL,
    `f_time`               time                           DEFAULT NULL,
    `f_tinyint`            tinyint                        DEFAULT NULL,
    `f_tinyint_unsigned`   tinyint unsigned               DEFAULT NULL,
    `f_json`               json                           DEFAULT NULL,
    `f_year`               year                           DEFAULT NULL,
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  AUTO_INCREMENT = 2
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci;

CREATE TABLE mysql_cdc_e2e_source_table2
(
    `id`                   int       NOT NULL AUTO_INCREMENT,
    `f_binary`             binary(64)                     DEFAULT NULL,
    `f_blob`               blob,
    `f_long_varbinary`     mediumblob,
    `f_longblob`           longblob,
    `f_tinyblob`           tinyblob,
    `f_varbinary`          varbinary(100)                 DEFAULT NULL,
    `f_smallint`           smallint                       DEFAULT NULL,
    `f_smallint_unsigned`  smallint unsigned              DEFAULT NULL,
    `f_mediumint`          mediumint                      DEFAULT NULL,
    `f_mediumint_unsigned` mediumint unsigned             DEFAULT NULL,
    `f_int`                int                            DEFAULT NULL,
    `f_int_unsigned`       int unsigned                   DEFAULT NULL,
    `f_integer`            int                            DEFAULT NULL,
    `f_integer_unsigned`   int unsigned                   DEFAULT NULL,
    `f_bigint`             bigint                         DEFAULT NULL,
    `f_bigint_unsigned`    bigint unsigned                DEFAULT NULL,
    `f_numeric`            decimal(10, 0)                 DEFAULT NULL,
    `f_decimal`            decimal(10, 0)                 DEFAULT NULL,
    `f_float`              float                          DEFAULT NULL,
    `f_double`             double                         DEFAULT NULL,
    `f_double_precision`   double                         DEFAULT NULL,
    `f_longtext`           longtext,
    `f_mediumtext`         mediumtext,
    `f_text`               text,
    `f_tinytext`           tinytext,
    `f_varchar`            varchar(100)                   DEFAULT NULL,
    `f_date`               date                           DEFAULT NULL,
    `f_datetime`           datetime                       DEFAULT NULL,
    `f_timestamp`          timestamp NULL                 DEFAULT NULL,
    `f_bit1`               bit(1)                         DEFAULT NULL,
    `f_bit64`              bit(64)                        DEFAULT NULL,
    `f_char`               char(1)                        DEFAULT NULL,
    `f_enum`               enum ('enum1','enum2','enum3') DEFAULT NULL,
    `f_mediumblob`         mediumblob,
    `f_long_varchar`       mediumtext,
    `f_real`               double                         DEFAULT NULL,
    `f_time`               time                           DEFAULT NULL,
    `f_tinyint`            tinyint                        DEFAULT NULL,
    `f_tinyint_unsigned`   tinyint unsigned               DEFAULT NULL,
    `f_json`               json                           DEFAULT NULL,
    `f_year`               year                           DEFAULT NULL,
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  AUTO_INCREMENT = 2
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci;
