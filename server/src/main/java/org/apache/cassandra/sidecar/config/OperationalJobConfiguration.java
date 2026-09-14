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
 * Configuration for operational jobs managed by Sidecar
 */
public interface OperationalJobConfiguration
{
    /**
     * @return the time-to-live for operational job tables
     */
    SecondBoundConfiguration tablesTtl();

    /**
     * @return whether operational job coordination is enabled for this Sidecar instance. When {@code true}, a
     * storage-backed coordinator enforces mutual exclusion for coordinated cluster-wide operations and the local
     * coordinator drives this Sidecar's per-node job execution; when {@code false}, coordination is disabled and
     * coordinated operations are rejected.
     */
    boolean coordinationEnabled();

    /**
     * @return whether operational jobs are tracked durably in Cassandra-backed storage. When {@code false}
     * (the default), jobs are tracked in memory only. Enable to persist job state across Sidecar restarts;
     * this requires the Sidecar operational-job schema to be available.
     */
    boolean durableTrackingEnabled();

    /**
     * @return the polling interval for the local job coordinator
     */
    SecondBoundConfiguration localJobCoordinationDelay();

    /**
     * @return the maximum time a node's local job may execute before it is marked failed
     */
    SecondBoundConfiguration nodeExecutionTimeout();

    /**
     * @return configuration for rolling restart operations
     */
    RollingRestartConfiguration rollingRestartConfiguration();
}
