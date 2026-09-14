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
import java.util.Objects;
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
    private final String targetDatacenter;

    public RollingRestartJob(UUID jobId,
                             List<List<UUID>> nodeExecutionOrder,
                             Map<String, String> operationMetadata,
                             String targetDatacenter)
    {
        super(jobId);
        this.nodeExecutionOrder = nodeExecutionOrder;
        this.operationMetadata = operationMetadata;
        this.targetDatacenter = targetDatacenter;
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
        // A rolling restart is a distributed cluster-wide operation whose work finishes on other nodes.
        // The manager must not release the active-operation lock when this initiating node's job completes;
        // the coordinator finalizer clears it once every node reaches a terminal state.
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
        return sameOperationJobs.stream()
                                .filter(job -> job instanceof RollingRestartJob)
                                .map(job -> (RollingRestartJob) job)
                                // Objects.equals tolerates a null targetDatacenter (a job targeting the local DC)
                                .anyMatch(job -> Objects.equals(targetDatacenter, job.targetDatacenter()));
    }

    @Override
    protected Future<Void> executeInternal()
    {
        return Future.succeededFuture();
    }

    /**
     * Unlike the self-tracking single-node jobs (e.g. {@code NodeDrainJob}, which derive their status from the live 
     * node operation mode, a rolling restart is a fire-and-forget handoff: this in-memory job only signals via 
     * {@link #executeInternal()} that the work has been handed off to the per-node coordinators, then it is evicted 
     * from the tracker almost immediately. It owns no terminal state of its own, as the operation's actual outcome 
     * is finalized by the LocalJobCoordinator.
     * <p>
     * Once the handoff succeeds, the async result completes successfully, but the distributed work keeps running in
     * the per-node coordinators, so {@code RUNNING} remains accurate for the brief in-memory window before eviction
     * (keeping the POST /restart response at 202/RUNNING). Once evicted, {@code GET /operational-jobs/:id} serves
     * the terminal status from storage. However, a job that failed <em>before</em> handoff (e.g. rejected by a
     * coordination conflict) must report {@link OperationalJobStatus#FAILED}, otherwise it would be persisted 
     * as {@code RUNNING} even though it never started.
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
     * @return the pre-computed execution order: sequential groups of parallel nodes
     */
    public List<List<UUID>> nodeExecutionOrder()
    {
        return nodeExecutionOrder;
    }

    /**
     * @return metadata associated with this restart operation
     */
    public Map<String, String> operationMetadata()
    {
        return operationMetadata;
    }

    @Override
    public String targetDatacenter()
    {
        return targetDatacenter;
    }
}
