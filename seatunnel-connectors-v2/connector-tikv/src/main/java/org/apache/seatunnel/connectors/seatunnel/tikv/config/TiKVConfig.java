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

package org.apache.seatunnel.connectors.seatunnel.tikv.config;

import java.io.Serializable;

public class TiKVConfig implements Serializable {

    public static final String NAME = "TiKV";

    public static final String HOST = "host";

    public static final String PD_PORT = "port";

    public static final String DATA_TYPE = "data_type";

    public static final String KEYWORD = "keyword";

    /**
     * keywords are xxx,xxx,xxx
     */
    public static final String KEYWORDS = "keywords";

    public static final String RANGES = "ranges";

    public static final String LIMIT = "limit";

    public static final String FORMAT = "format";

    public static final Integer LIMIT_DEFAULT = 10_000;

    public static final String CHECK_ERROR_FORMAT = "please check the input field format is [%s]";

    public static final String SINK_ERROR_INFO = "TiKV sink data only data type is : key";

}
