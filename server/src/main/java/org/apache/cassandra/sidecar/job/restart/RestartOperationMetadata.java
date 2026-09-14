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

package org.apache.cassandra.sidecar.job.restart;

/**
 * Keys for rolling restart job metadata.
 */
public final class RestartOperationMetadata
{
    /** Per-request override (seconds) for the max wait for the ring to be healthy before stopping a node. */
    public static final String CASSANDRA_HEALTH_TIMEOUT_SECONDS = "cassandraHealthTimeoutSeconds";
    /** Per-request override (seconds) for the max wait for a single stop/start to converge. */
    public static final String NODE_STATE_TRANSITION_TIMEOUT_SECONDS = "nodeStateTransitionTimeoutSeconds";
    /** Per-request override for the number of restart retry attempts per node. */
    public static final String NODE_RESTART_RETRY_ATTEMPTS = "nodeRestartRetryAttempts";
    /** Per-request override (seconds) for the minimum settle time between execution groups. Zero disables it. */
    public static final String WAIT_BETWEEN_EXECUTION_GROUPS_SECONDS = "waitBetweenExecutionGroupsSeconds";
    /** Effective number of nodes restarted together at peak (the size of the largest execution group). */
    public static final String EFFECTIVE_MAX_RESTART_PARALLELISM = "effectiveMaxRestartParallelism";

    private RestartOperationMetadata()
    {
        throw new UnsupportedOperationException("Do not instantiate.");
    }
}
