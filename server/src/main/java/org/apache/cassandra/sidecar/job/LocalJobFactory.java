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

package org.apache.cassandra.sidecar.job;

import java.util.Map;
import java.util.UUID;

import org.apache.cassandra.sidecar.common.data.OperationType;
import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;
import org.jetbrains.annotations.Nullable;

/**
 * Factory for creating {@link LocalJob} instances for cluster-wide operations.
 * Each factory handles a single {@link OperationType}; the coordinator selects the factory for an
 * operation's type. 
 */
public interface LocalJobFactory
{
    /**
     * Creates a new {@link LocalJob} for the given operation.
     *
     * @param operationId   the cluster-wide operation identifier
     * @param nodeId        the local node's host identifier
     * @param instanceHost  the hostname of the local Cassandra instance
     * @param operationType the type of operation
     * @return a new local job instance
     */
    LocalJob createJob(UUID operationId, UUID nodeId, String instanceHost, OperationType operationType);

    /**
     * Creates a new {@link LocalJob} for the given operation, with access to the operation's metadata.
     *
     * <p>The default implementation ignores {@code operationMetadata} and delegates to
     * {@link #createJob(UUID, UUID, String, OperationType)}. Factories whose jobs honor per-operation
     * parameters (e.g. request-supplied overrides) can override this method.</p>
     *
     * @param operationId       the cluster-wide operation identifier
     * @param nodeId            the local node's host identifier
     * @param instanceHost      the hostname of the local Cassandra instance
     * @param operationType     the type of operation
     * @param operationMetadata the operation's metadata, or {@code null} when none was persisted
     * @return a new local job instance
     */
    default LocalJob createJob(UUID operationId, UUID nodeId, String instanceHost, OperationType operationType,
                               Map<String, String> operationMetadata)
    {
        return createJob(operationId, nodeId, instanceHost, operationType);
    }

    /**
     * Whether a node requires its predecessors to have succeeded before it may execute. When {@code true},
     * a failed predecessor blocks subsequent parallel groups from executing.
     *
     * @return {@code true} if subsequent groups should not proceed when a predecessor fails
     */
    default boolean requiresPredecessorSuccess()
    {
        return false;
    }

    /**
     * Whether this factory's job can be safely re-executed after a Sidecar crash.
     * If {@code true}, the coordinator resets the node status and resubmits the job.
     * If {@code false}, the coordinator marks the node as FAILED.
     *
     * <p>Defaults to {@code false} so that jobs whose idempotency is unknown are not silently re-executed.</p>
     *
     * @return {@code true} if the job can be safely re-executed
     */
    default boolean canRecover()
    {
        return false;
    }

    /**
     * The maximum time this factory's job may execute before the coordinator marks the node as failed.
     *
     * <p>Returning {@code null} (the default) uses the coordinator's configured default
     * ({@code operational_job.node_execution_timeout}). A factory whose job has its own natural bound can
     * override this.</p>
     *
     * @return the per-operation execution timeout, or {@code null} to use the configured default
     */
    @Nullable
    default SecondBoundConfiguration nodeExecutionTimeout()
    {
        return null;
    }

    /**
     * The minimum time that must elapse after an execution group finishes before the coordinator may start the next
     * group. This is a settle window for the nodes just operated on. For example, during a restart, this lets caches
     * warm before restarting the next group of nodes.
     *
     * <p>Returning {@code null} (the default) means this factory's job needs no settle window and groups follow one
     * another as soon as predecessors are terminal. A zero duration is equivalent.</p>
     *
     * @param operationMetadata the operation's metadata, or {@code null} when none was persisted, so a factory can
     *                          honor a per-request override
     * @return the minimum wait between execution groups, or {@code null} for no wait
     */
    @Nullable
    default SecondBoundConfiguration waitBetweenExecutionGroups(@Nullable Map<String, String> operationMetadata)
    {
        return null;
    }
}
