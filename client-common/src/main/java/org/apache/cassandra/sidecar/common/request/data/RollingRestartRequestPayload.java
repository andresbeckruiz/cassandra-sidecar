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

import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Request payload for rolling restart operations.
 *
 * <p>Valid JSON:</p>
 * <pre>
 *   {
 *     "datacenter": "dc1",
 *     "nodes": ["uuid-1", "uuid-2"],
 *     "maxRestartParallelism": 2,
 *     "cassandraHealthTimeoutSeconds": 120,
 *     "nodeStateTransitionTimeoutSeconds": 300,
 *     "nodeRestartRetryAttempts": 3,
 *     "waitBetweenExecutionGroupsSeconds": 180
 *   }
 * </pre>
 *
 * <p>An omitted or empty {@code nodes} list restarts every node in the datacenter. The optional overrides, when
 * supplied, must be in range; otherwise the configured defaults apply. {@code cassandraHealthTimeoutSeconds} and
 * {@code nodeStateTransitionTimeoutSeconds} must be greater than zero. {@code nodeRestartRetryAttempts} counts retries
 * beyond the first attempt, so it must not be negative; zero means restart once with no retry.
 * {@code waitBetweenExecutionGroupsSeconds} is the minimum settle time between execution groups; zero disables the
 * wait, and it must be no greater than 1800 (30 minutes), since an oversized settle window would hold the
 * datacenter's operation lock for its whole duration.</p>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RollingRestartRequestPayload
{
    private final String datacenter;
    private final List<String> nodes;
    private final int maxRestartParallelism;
    private final Integer cassandraHealthTimeoutSeconds;
    private final Integer nodeStateTransitionTimeoutSeconds;
    private final Integer nodeRestartRetryAttempts;
    private final Integer waitBetweenExecutionGroupsSeconds;

    @JsonCreator
    public RollingRestartRequestPayload(@JsonProperty(value = "datacenter") String datacenter,
                                        @JsonProperty(value = "nodes") List<String> nodes,
                                        @JsonProperty(value = "maxRestartParallelism") Integer maxRestartParallelism,
                                        @JsonProperty(value = "cassandraHealthTimeoutSeconds") Integer cassandraHealthTimeoutSeconds,
                                        @JsonProperty(value = "nodeStateTransitionTimeoutSeconds") Integer nodeStateTransitionTimeoutSeconds,
                                        @JsonProperty(value = "nodeRestartRetryAttempts") Integer nodeRestartRetryAttempts,
                                        @JsonProperty(value = "waitBetweenExecutionGroupsSeconds") Integer waitBetweenExecutionGroupsSeconds)
    {
        this.datacenter = datacenter;
        this.nodes = nodes != null ? nodes : Collections.emptyList();
        this.maxRestartParallelism = maxRestartParallelism != null ? maxRestartParallelism : 1;
        this.cassandraHealthTimeoutSeconds = cassandraHealthTimeoutSeconds;
        this.nodeStateTransitionTimeoutSeconds = nodeStateTransitionTimeoutSeconds;
        this.nodeRestartRetryAttempts = nodeRestartRetryAttempts;
        this.waitBetweenExecutionGroupsSeconds = waitBetweenExecutionGroupsSeconds;
    }

    @JsonProperty("datacenter")
    public String datacenter()
    {
        return datacenter;
    }

    @JsonProperty("nodes")
    public List<String> nodes()
    {
        return nodes;
    }

    @JsonProperty("maxRestartParallelism")
    public int maxRestartParallelism()
    {
        return maxRestartParallelism;
    }

    @JsonProperty("cassandraHealthTimeoutSeconds")
    public Integer cassandraHealthTimeoutSeconds()
    {
        return cassandraHealthTimeoutSeconds;
    }

    @JsonProperty("nodeStateTransitionTimeoutSeconds")
    public Integer nodeStateTransitionTimeoutSeconds()
    {
        return nodeStateTransitionTimeoutSeconds;
    }

    @JsonProperty("nodeRestartRetryAttempts")
    public Integer nodeRestartRetryAttempts()
    {
        return nodeRestartRetryAttempts;
    }

    @JsonProperty("waitBetweenExecutionGroupsSeconds")
    public Integer waitBetweenExecutionGroupsSeconds()
    {
        return waitBetweenExecutionGroupsSeconds;
    }

    @Override
    public String toString()
    {
        return "RollingRestartRequestPayload{datacenter='" + datacenter + "'"
               + ", nodes=" + nodes
               + ", maxRestartParallelism=" + maxRestartParallelism
               + ", cassandraHealthTimeoutSeconds=" + cassandraHealthTimeoutSeconds
               + ", nodeStateTransitionTimeoutSeconds=" + nodeStateTransitionTimeoutSeconds
               + ", nodeRestartRetryAttempts=" + nodeRestartRetryAttempts
               + ", waitBetweenExecutionGroupsSeconds=" + waitBetweenExecutionGroupsSeconds + "}";
    }
}
