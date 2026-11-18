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

import java.util.List;
import java.util.Map;
import java.util.UUID;

import io.vertx.core.Future;
import org.apache.cassandra.sidecar.common.data.OperationType;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.apache.cassandra.sidecar.job.OperationalJob;
import org.jetbrains.annotations.NotNull;

/**
 * Cluster-wide OperationalJob representing a rolling restart operation.
 */
public class RollingRestartJob extends OperationalJob
{
    private final List<List<UUID>> nodeExecutionOrder;
    private final Map<String, String> operationMetadata;

    public RollingRestartJob(UUID jobId, List<List<UUID>> nodeExecutionOrder, Map<String, String> operationMetadata)
    {
        super(jobId);
        this.nodeExecutionOrder = nodeExecutionOrder;
        this.operationMetadata = operationMetadata;
    }

    @Override
    public OperationType operationType()
    {
        return OperationType.RESTART;
    }

    @Override
    public boolean requiresCoordination()
    {
        return true;
    }

    @Override
    public boolean releasesOnCompletion()
    {
        // The manager must not release the active-operation lock when this initiating node's job completes.
        // Instead, the coordinator finalizes and clears the lock once every node reaches a terminal state.
        return false;
    }

    @Override
    public String name()
    {
        return "RollingRestart";
    }

    @Override
    public boolean hasConflict(@NotNull List<OperationalJob> sameOperationJobs)
    {
        return !sameOperationJobs.isEmpty();
    }

    @Override
    protected Future<Void> executeInternal()
    {
        return Future.succeededFuture();
    }

    /**
     * This in-memory job only hands the work off to the LocalJobCoordinator and is evicted from the tracker
     * soon after. The operation's outcome is finalized by the coordinator and read from storage.
     * <p>
     * The handoff completes the async result successfully while the restart is still running, so this job reports
     * {@code RUNNING} instead of {@code SUCCEEDED}. A job that failed before the handoff (for example, rejected by a
     * coordination conflict) reports {@link OperationalJobStatus#FAILED}, so that it is not persisted as
     * {@code RUNNING}.
     */
    @Override
    public OperationalJobStatus status()
    {
        Future<Void> result = asyncResult();
        if (result.isComplete() && result.failed())
        {
            return OperationalJobStatus.FAILED;
        }
        return OperationalJobStatus.RUNNING;
    }

    /**
     * @return the node execution order
     */
    @Override
    public List<List<UUID>> nodeExecutionOrder()
    {
        return nodeExecutionOrder;
    }

    /**
     * @return metadata associated with this restart operation
     */
    @Override
    public Map<String, String> operationMetadata()
    {
        return operationMetadata;
    }
}
