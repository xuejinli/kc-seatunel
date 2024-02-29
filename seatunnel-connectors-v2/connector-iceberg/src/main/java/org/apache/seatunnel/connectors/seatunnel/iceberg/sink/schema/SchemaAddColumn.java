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

package org.apache.seatunnel.connectors.seatunnel.iceberg.sink.schema;

import org.apache.iceberg.types.Type;

public class SchemaAddColumn implements ISchemaChange {
    private final String parentName;
    private final String name;
    private final Type type;

    public SchemaAddColumn(String parentName, String name, Type type) {
        this.parentName = parentName;
        this.name = name;
        this.type = type;
    }

    public String parentName() {
        return parentName;
    }

    public String name() {
        return name;
    }

    public String key() {
        return parentName == null ? name : parentName + "." + name;
    }

    public Type type() {
        return type;
    }
}
