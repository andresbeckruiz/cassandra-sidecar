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

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.apache.cassandra.sidecar.concurrent.TaskExecutorPool;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.exceptions.OperationalJobConflictException;
import org.apache.cassandra.sidecar.exceptions.OperationalJobNotCoordinatedException;
import org.apache.cassandra.sidecar.exceptions.OperationalJobNotFoundException;
import org.apache.cassandra.sidecar.job.storage.OperationalJobRecord;
import org.apache.cassandra.sidecar.job.storage.StorageProvider;
import org.apache.cassandra.sidecar.utils.InvocationTrackingFunction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A durable implementation of {@link OperationalJobTracker} that persists job state
 * via a {@link StorageProvider}. A local {@link ConcurrentHashMap} caches live
 * {@link OperationalJob} references for the current process, since executing jobs
 * with Vert.x promises cannot be reconstituted from storage. Once a job completes,
 * it is removed from the local map, and subsequent lookups are served from storage.
 *
 * <p>Storage currently receives only two writes per job: the initial
 * {@link OperationalJobStatus#CREATED CREATED} record on submission and the terminal status on
 * completion. As a result, if the sidecar restarts mid-operation, or if the
 * terminal-status update exhausts its retries (see {@link #updateTerminalStatus}), the persisted
 * record can remain stuck at {@code CREATED} even though the operation has since progressed or
 * finished. There is currently no marker distinguishing a record that is genuinely still
 * {@code CREATED} from one whose true state was simply never recorded; adding such a marker together
 * with a reconciliation sweep (leveraging {@link StorageProvider#findAllJobs(int)}) is tracked as
 * follow-up work in
 * <a href="https://issues.apache.org/jira/browse/CASSSIDECAR-482">CASSSIDECAR-482</a>.
 */
@Singleton
public class DurableOperationalJobTracker implements OperationalJobTracker
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DurableOperationalJobTracker.class);
    private static final String OPERATOR_ABORT_REASON = "Aborted by operator request";
    private static final int MAX_STORAGE_WRITE_ATTEMPTS = 3;
    private static final long RETRY_DELAY_MS = 100;
    private static final long RETRY_JITTER_MS = 100;

    private final ConcurrentHashMap<UUID, OperationalJob> liveJobs;
    private final StorageProvider storageProvider;
    private final TaskExecutorPool executor;

    @Inject
    public DurableOperationalJobTracker(ServiceConfiguration serviceConfiguration,
                                        StorageProvider storageProvider,
                                        TaskExecutorPool executor)
    {
        this.liveJobs = new ConcurrentHashMap<>(serviceConfiguration.operationalJobTrackerSize());
        this.storageProvider = storageProvider;
        this.executor = executor;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OperationalJob computeIfAbsent(UUID jobId, Function<UUID, OperationalJob> mappingFunction)
    {
        InvocationTrackingFunction<UUID, OperationalJob> mappingFunctionTracker =
        new InvocationTrackingFunction<>(mappingFunction);
        OperationalJob job = liveJobs.computeIfAbsent(jobId, mappingFunctionTracker);

        if (mappingFunctionTracker.wasInvoked())
        {
            boolean activatingCoordinatedJob = job.requiresCoordination()
                                               && job.nodeExecutionOrder() != null
                                               && !job.asyncResult().isComplete();
            persistInitialRecord(job, activatingCoordinatedJob, 1);
        }

        return job;
    }

    private void persistInitialRecord(OperationalJob job, boolean activatingCoordinatedJob, int attempt)
    {
        executor.runBlocking(() -> {
                    storageProvider.persistJob(OperationalJobRecord.fromOperationalJob(job));
                    if (activatingCoordinatedJob)
                    {
                        seedNodeStatuses(job);
                        // Marked here because we currently only support job submission that executes immediately.
                        storageProvider.updateJobStatus(job.jobId(), job.operationType(),
                                                        OperationalJobStatus.RUNNING, null);
                    }
                })
                .onSuccess(v -> job.asyncResult().onComplete(ar -> {
                    // A coordinated job outlives this process's handoff; the coordinator owns its terminal status
                    if (!job.requiresCoordination())
                    {
                        updateTerminalStatus(job);
                    }
                    liveJobs.remove(job.jobId());
                }))
                .onFailure(e -> {
                    LOGGER.warn("Failed to persist job {} to storage (attempt {}/{}). error={}",
                                job.jobId(), attempt, MAX_STORAGE_WRITE_ATTEMPTS, e.getMessage());
                    if (attempt < MAX_STORAGE_WRITE_ATTEMPTS)
                    {
                        // Add jitter so that concurrent sidecar processes retrying against the same transient
                        // Cassandra blip do not all retry in lockstep
                        long delay = RETRY_DELAY_MS * attempt + ThreadLocalRandom.current().nextLong(RETRY_JITTER_MS);
                        executor.setTimer(delay,
                                          id -> persistInitialRecord(job, activatingCoordinatedJob, attempt + 1));
                    }
                    else
                    {
                        // Persist exhausted its retries, but the job is already executing on a separate executor.
                        // Keep it in liveJobs so in-process status queries and conflict detection still see it.
                        LOGGER.error("Failed to persist job {} to storage after {} attempts. "
                                     + "Job will be tracked in-memory only.", job.jobId(), MAX_STORAGE_WRITE_ATTEMPTS, e);
                        job.asyncResult().onComplete(ar -> liveJobs.remove(job.jobId()));
                    }
                });
    }

    @Nullable
    @Override
    public OperationalJobInfo get(UUID jobId)
    {
        OperationalJob liveJob = liveJobs.get(jobId);
        if (liveJob != null)
        {
            return liveJob;
        }

        OperationalJobRecord record = storageProvider.findJob(jobId);
        if (record == null)
        {
            return null;
        }
        return enrichWithNodeStatuses(record);
    }

    @NotNull
    @Override
    public Map<UUID, OperationalJob> jobsView()
    {
        return Collections.unmodifiableMap(liveJobs);
    }

    @NotNull
    @Override
    public List<OperationalJob> inflightJobsByOperation(String operation)
    {
        return liveJobs.values()
                       .stream()
                       .filter(j -> j.name().equals(operation) &&
                                    (j.status() == OperationalJobStatus.RUNNING ||
                                     j.status() == OperationalJobStatus.CREATED))
                       .collect(Collectors.toList());
    }

    @NotNull
    @Override
    public List<OperationalJobInfo> inflightJobs()
    {
        // In-process live jobs take precedence, as they carry the authoritative in-memory lifecycle state
        Map<UUID, OperationalJobInfo> merged = new LinkedHashMap<>(liveJobs);

        // Coordinated jobs are evicted from liveJobs almost immediately after handoff, yet the operation keeps 
        // running for minutes via the local coordinator. Surface those still-active operations from storage so 
        // they remain visible in the list. getActiveOperations() is scoped to the local datacenter.
        for (UUID activeJobId : storageProvider.getActiveOperations().values())
        {
            if (merged.containsKey(activeJobId))
            {
                continue;
            }
            OperationalJobRecord record = storageProvider.findJob(activeJobId);
            if (record != null)
            {
                merged.put(activeJobId, enrichWithNodeStatuses(record));
            }
        }

        return merged.values()
                     .stream()
                     .filter(job -> !job.status().isCompleted())
                     .collect(Collectors.toList());
    }

    /**
     * {@inheritDoc}
     *
     * <p>The record is read from storage rather than from {@link #liveJobs}, because an in-process job reports
     * the outcome of the local handoff rather than the state of the cluster-wide operation the abort acts on.</p>
     */
    @Override
    public OperationalJobInfo markJobAsAborted(UUID jobId, boolean force)
    {
        OperationalJobRecord record = storageProvider.findJob(jobId);
        if (record == null)
        {
            throw new OperationalJobNotFoundException("Unknown job with ID: " + jobId);
        }

        if (record.status() == OperationalJobStatus.SUCCEEDED || record.status() == OperationalJobStatus.FAILED)
        {
            throw new OperationalJobConflictException("Job " + jobId + " has already finished with status "
                                                      + record.status() + " and cannot be aborted");
        }

        // Single-node jobs are persisted here too, and are told apart from coordinated cluster-wide operations by
        // whether they recorded an execution order.
        List<List<UUID>> nodeExecutionOrder = record.nodeExecutionOrder();
        if (nodeExecutionOrder == null || nodeExecutionOrder.isEmpty())
        {
            throw new OperationalJobNotCoordinatedException(
            "Job " + jobId + " is not a coordinated cluster-wide operation, so it holds no datacenter lock and has "
            + "no per-node state to settle");
        }

        LOGGER.warn("Aborting operational job by request. jobId={} operationType={} priorStatus={} force={}",
                    jobId, record.operationType(), record.status(), force);

        storageProvider.updateJobStatus(jobId, record.operationType(), OperationalJobStatus.ABORTED,
                                        OPERATOR_ABORT_REASON);

        Map<UUID, OperationalJobStatus> nodeStatuses = storageProvider.getNodeStatusesForOperation(jobId);
        if (force)
        {
            forceSettleOutstandingNodes(jobId, OperationalJobStatus.ABORTED, nodeExecutionOrder, nodeStatuses);
        }
        return abortedRecord(record, nodeStatuses);
    }

    /**
     * Settles every node row that has not reached a terminal status, standing in for the Sidecars that cannot
     * settle their own. {@link NodeSettlement} decides the status each node records, the same rule the Sidecar
     * that owns a node applies to the rows it settles itself.
     *
     * <p><b>This writes rows this Sidecar does not own, so operators must know that the lock can be released while
     * a job is still running when they send a force request.</b></p>
     *
     * <p>A node abandoned mid-execution gets no {@code onJobFailed} reconciliation, because the Sidecar that could
     * run it is the one presumed unreachable. Those nodes are logged so the operator knows which ones they have
     * taken responsibility for.</p>
     */
    private void forceSettleOutstandingNodes(UUID jobId,
                                             OperationalJobStatus operationStatus,
                                             List<List<UUID>> nodeExecutionOrder,
                                             Map<UUID, OperationalJobStatus> nodeStatuses)
    {
        List<UUID> abandonedMidExecution = new ArrayList<>();
        List<UUID> neverStarted = new ArrayList<>();
        for (List<UUID> group : nodeExecutionOrder)
        {
            for (UUID nodeId : group)
            {
                switch (NodeSettlement.of(nodeStatuses.get(nodeId)))
                {
                    case NOT_STARTED:
                        neverStarted.add(nodeId);
                        break;
                    case ABANDONED_MID_EXECUTION:
                        abandonedMidExecution.add(nodeId);
                        break;
                    default:
                        break;
                }
            }
        }

        settleAll(jobId, neverStarted, NodeSettlement.NOT_STARTED.terminalStatus(operationStatus), nodeStatuses);
        settleAll(jobId, abandonedMidExecution,
                  NodeSettlement.ABANDONED_MID_EXECUTION.terminalStatus(operationStatus), nodeStatuses);

        LOGGER.warn("Forced abort settled node rows on behalf of the Sidecars that own them. jobId={} "
                    + "neverStarted={} abandonedMidExecution={}. The nodes abandoned mid-execution were executing "
                    + "when the operation was forced, and nothing will reconcile them; check them by hand.",
                    jobId, neverStarted, abandonedMidExecution);
    }

    private void settleAll(UUID jobId, List<UUID> nodeIds, OperationalJobStatus status,
                           Map<UUID, OperationalJobStatus> nodeStatuses)
    {
        if (nodeIds.isEmpty())
        {
            return;
        }
        storageProvider.updateNodeStatuses(jobId, nodeIds, status);
        nodeIds.forEach(nodeId -> nodeStatuses.put(nodeId, status));
    }

    /**
     * The job as it stands once aborted, carrying the per-node breakdown so the caller sees how far the operation
     * had got before it was given up on. A non-forced abort leaves the nodes it did not settle still pending or
     * executing, and those are the ones the Sidecars that own them have yet to wind down.
     */
    private OperationalJobRecord abortedRecord(OperationalJobRecord record,
                                               Map<UUID, OperationalJobStatus> nodeStatuses)
    {
        return withNodeStatuses(record, OperationalJobStatus.ABORTED, Instant.now(), OPERATOR_ABORT_REASON,
                                nodeStatuses);
    }

    /**
     * Seeds a {@link OperationalJobStatus#CREATED} row in node state storage for every node in the job's
     * execution order.
     */
    private void seedNodeStatuses(OperationalJob job)
    {
        List<List<UUID>> executionOrder = job.nodeExecutionOrder();
        if (executionOrder == null)
        {
            return;
        }
        List<UUID> nodeIds = executionOrder.stream()
                                           .flatMap(List::stream)
                                           .collect(Collectors.toList());
        if (nodeIds.isEmpty())
        {
            return;
        }
        storageProvider.updateNodeStatuses(job.jobId(), nodeIds, OperationalJobStatus.CREATED);
    }

    /**
     * Enriches an {@link OperationalJobRecord} with per-node status data from storage.
     * If the record already has non-empty node lists (e.g. populated by a storage provider
     * that joins the data in a single query), the record is returned as is.
     */
    private OperationalJobRecord enrichWithNodeStatuses(OperationalJobRecord record)
    {
        if (!record.nodesPending().isEmpty()
            || !record.nodesExecuting().isEmpty()
            || !record.nodesSucceeded().isEmpty()
            || !record.nodesFailed().isEmpty()
            || !record.nodesAborted().isEmpty())
        {
            return record;
        }

        Map<UUID, OperationalJobStatus> nodeStatuses =
        storageProvider.getNodeStatusesForOperation(record.jobId());
        if (nodeStatuses.isEmpty())
        {
            return record;
        }

        return withNodeStatuses(record, record.status(), record.lastUpdate(), record.failureReason(), nodeStatuses);
    }

    /**
     * Rebuilds a record with the per-node breakdown derived from {@code nodeStatuses}.
     */
    private static OperationalJobRecord withNodeStatuses(OperationalJobRecord record,
                                                         OperationalJobStatus status,
                                                         @Nullable Instant lastUpdate,
                                                         @Nullable String failureReason,
                                                         Map<UUID, OperationalJobStatus> nodeStatuses)
    {
        return OperationalJobRecord.builder()
                                   .jobId(record.jobId())
                                   .operationType(record.operationType())
                                   .status(status)
                                   .startTime(record.startTime())
                                   .lastUpdate(lastUpdate)
                                   .failureReason(failureReason)
                                   .nodeExecutionOrder(record.nodeExecutionOrder())
                                   .operationMetadata(record.operationMetadata())
                                   .nodesPending(nodesWith(nodeStatuses, OperationalJobStatus.CREATED))
                                   .nodesExecuting(nodesWith(nodeStatuses, OperationalJobStatus.RUNNING))
                                   .nodesSucceeded(nodesWith(nodeStatuses, OperationalJobStatus.SUCCEEDED))
                                   .nodesFailed(nodesWith(nodeStatuses, OperationalJobStatus.FAILED))
                                   .nodesAborted(nodesWith(nodeStatuses, OperationalJobStatus.ABORTED))
                                   .build();
    }

    private static List<UUID> nodesWith(Map<UUID, OperationalJobStatus> nodeStatuses, OperationalJobStatus status)
    {
        return nodeStatuses.entrySet()
                           .stream()
                           .filter(entry -> entry.getValue() == status)
                           .map(Map.Entry::getKey)
                           .collect(Collectors.collectingAndThen(Collectors.toList(),
                                                                 Collections::unmodifiableList));
    }

    /**
     * Attempts to update the terminal status in storage with retry.
     * If all attempts fail, logs a warning and continues.
     */
    private void updateTerminalStatus(OperationalJob job)
    {
        updateTerminalStatus(job, 1);
    }

    private void updateTerminalStatus(OperationalJob job, int attempt)
    {
        try
        {
            storageProvider.updateJobStatus(job.jobId(), job.operationType(), job.status(), job.failureReason());
        }
        catch (RuntimeException e)
        {
            LOGGER.warn("Failed to update terminal status for job {} (attempt {}/{}). error={}",
                        job.jobId(), attempt, MAX_STORAGE_WRITE_ATTEMPTS, e.getMessage());
            if (attempt < MAX_STORAGE_WRITE_ATTEMPTS)
            {
                // Add jitter so that concurrent sidecar processes retrying against the same transient
                // Cassandra blip do not all retry in lockstep
                long delay = RETRY_DELAY_MS * attempt + ThreadLocalRandom.current().nextLong(RETRY_JITTER_MS);
                executor.setTimer(delay, id -> updateTerminalStatus(job, attempt + 1));
            }
            else
            {
                LOGGER.error("Exhausted retries when updating terminal status for job {}. " +
                             "Manual intervention may be required to correct job metadata.", job.jobId(), e);
            }
        }
    }
}
