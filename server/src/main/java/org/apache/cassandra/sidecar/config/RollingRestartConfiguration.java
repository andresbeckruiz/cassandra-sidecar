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

package org.apache.cassandra.sidecar.config;

import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;

/**
 * Configuration for rolling restart operations
 */
public interface RollingRestartConfiguration
{
    /**
     * @return whether rolling restart operations are enabled
     */
    boolean enabled();

    /**
     * @return the maximum time to wait for Cassandra ring to become healthy before restarting a node
     */
    SecondBoundConfiguration cassandraHealthTimeout();

    /**
     * @return the maximum time to wait for a single node restart attempt (stop or start) to converge
     */
    SecondBoundConfiguration nodeStateTransitionTimeout();

    /**
     * @return the number of retry attempts for restarting a node
     */
    int nodeRestartRetryAttempts();

    /**
     * The minimum time to wait after an execution group finishes before the next group may start, giving the
     * just-restarted nodes time to warm their caches.
     *
     * <p>{@code 0s} disables the wait. The wait is enforced at the coordinator's polling granularity,
     * so the observed pause is at least this value and at most this value plus roughly one
     * {@code operational_job.local_job_coordination_delay}.</p>
     *
     * @return the minimum wait between execution groups
     */
    SecondBoundConfiguration waitBetweenExecutionGroups();
}
