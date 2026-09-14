/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.sidecar.common.request.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Request payload for updating the status of an existing operational job, in the JSON Patch form described by
 * RFC 6902.
 *
 * <p>Valid JSON:</p>
 * <pre>
 *   {
 *     "op": "replace",
 *     "path": "/status",
 *     "value": "ABORTED"
 *   }
 * </pre>
 *
 * <p>All three fields are required. {@code replace} is the only operation and {@code /status} the only path;
 * {@code ABORTED} is the only value currently supported.</p>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class UpdateOperationalJobRequestPayload
{
    /**
     * The only {@code op} this endpoint accepts.
     */
    public static final String REPLACE_OP = "replace";

    /**
     * The only {@code path} this endpoint accepts.
     */
    public static final String STATUS_PATH = "/status";

    private final String op;
    private final String path;
    private final String value;

    @JsonCreator
    public UpdateOperationalJobRequestPayload(@JsonProperty(value = "op") String op,
                                              @JsonProperty(value = "path") String path,
                                              @JsonProperty(value = "value") String value)
    {
        this.op = op;
        this.path = path;
        this.value = value;
    }

    @JsonProperty("op")
    public String op()
    {
        return op;
    }

    @JsonProperty("path")
    public String path()
    {
        return path;
    }

    @JsonProperty("value")
    public String value()
    {
        return value;
    }

    @Override
    public String toString()
    {
        return "UpdateOperationalJobRequestPayload{op='" + op + "', path='" + path + "', value='" + value + "'}";
    }
}
