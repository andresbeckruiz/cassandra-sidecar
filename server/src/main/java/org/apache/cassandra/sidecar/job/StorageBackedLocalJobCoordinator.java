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

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.data.OperationType;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.apache.cassandra.sidecar.common.server.utils.DurationSpec;
import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.OperationalJobConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.job.storage.OperationalJobRecord;
import org.apache.cassandra.sidecar.job.storage.StorageProvider;
import org.apache.cassandra.sidecar.tasks.PeriodicTask;
import org.apache.cassandra.sidecar.tasks.PeriodicTaskExecutor;
import org.apache.cassandra.sidecar.tasks.ScheduleDecision;
import org.apache.cassandra.sidecar.utils.EventBusUtils;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.cassandra.sidecar.utils.TimeProvider;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_CASSANDRA_CQL_READY;

/**
 * Polls for active cluster-wide operations and coordinates local job execution.
 * Implements {@link PeriodicTask} for scheduled polling and {@link LocalJobCoordinator}
 * for predecessor checking, status reporting, and job completion detection.
 */
@Singleton
public class StorageBackedLocalJobCoordinator implements LocalJobCoordinator, PeriodicTask
{
    private static final Logger LOGGER = LoggerFactory.getLogger(StorageBackedLocalJobCoordinator.class);
    // Gives about 5.5 minutes of retries, in case the local node or the instances selected by
    // SidecarLoadBalancingPolicy are not CQL-ready
    private static final int MAX_NODE_STATUS_RETRIES = 12;
    private static final long NODE_STATUS_RETRY_DELAY_MS = 5000;

    private final OperationalJobCoordinator operationalJobCoordinator;
    private final StorageProvider storageProvider;
    private final InstanceMetadataFetcher instanceMetadataFetcher;
    private final LocalJobManager localJobManager;
    private final OperationalJobConfiguration config;
    private final LocalJobFactory localJobFactory;
    private final long defaultNodeExecutionTimeoutMs;

    // When this Sidecar first saw a group's predecessors all reach a terminal state, keyed by operation and group.
    private final Map<GroupKey, Long> predecessorsCompletedAtNanos = new ConcurrentHashMap<>();
    private final DurationSpec randomizedInitialDelay;
    private final ExecutorPools executorPools;
    private final TimeProvider timeProvider;

    @Inject
    public StorageBackedLocalJobCoordinator(ExecutorPools executorPools,
                                            OperationalJobCoordinator operationalJobCoordinator,
                                            StorageProvider storageProvider,
                                            InstanceMetadataFetcher instanceMetadataFetcher,
                                            LocalJobManager localJobManager,
                                            SidecarConfiguration configuration,
                                            LocalJobFactory localJobFactory,
                                            TimeProvider timeProvider)
    {
        this.executorPools = executorPools;
        this.timeProvider = timeProvider;
        this.operationalJobCoordinator = operationalJobCoordinator;
        this.storageProvider = storageProvider;
        this.instanceMetadataFetcher = instanceMetadataFetcher;
        this.localJobManager = localJobManager;
        this.config = configuration.operationalJobConfiguration();
        this.localJobFactory = localJobFactory;
        this.defaultNodeExecutionTimeoutMs = config.nodeExecutionTimeout().toMillis();

        long delaySeconds = config.localJobCoordinationDelay().toSeconds();
        // nextLong bound is exclusive, so +1 makes the range [0, delaySeconds] inclusive
        long randomSeconds = ThreadLocalRandom.current().nextLong(delaySeconds + 1);
        this.randomizedInitialDelay = SecondBoundConfiguration.parse(Math.max(1, randomSeconds) + "s");
    }

    @Override
    public void deploy(Vertx vertx, PeriodicTaskExecutor executor)
    {
        EventBusUtils.onceLocalConsumer(vertx.eventBus(), ON_CASSANDRA_CQL_READY.address(),
                                        ignored -> {
                                            storageProvider.initialize();
                                            executor.schedule(this);
                                        });
    }

    @Override
    public DurationSpec delay()
    {
        return config.localJobCoordinationDelay();
    }

    @Override
    public DurationSpec initialDelay()
    {
        return randomizedInitialDelay;
    }

    @Override
    public ScheduleDecision scheduleDecision()
    {
        if (!config.coordinationEnabled())
        {
            return ScheduleDecision.SKIP;
        }
        // Skip until the sidecar schema is ready; avoids dispatching a no-op blocking run.
        return storageProvider.isAvailable() ? ScheduleDecision.EXECUTE : ScheduleDecision.SKIP;
    }

    @Override
    public void execute(Promise<Void> promise)
    {
        try
        {
            if (!storageProvider.isAvailable())
            {
                promise.complete();
                return;
            }

            Map<OperationType, UUID> activeOps = operationalJobCoordinator.getActiveOperations();
            if (activeOps.isEmpty())
            {
                resetState();
                promise.complete();
                return;
            }

            abandonOperationsWithoutLock(activeOps.values());

            for (Map.Entry<OperationType, UUID> entry : activeOps.entrySet())
            {
                OperationType operationType = entry.getKey();
                UUID operationId = entry.getValue();

                OperationalJobRecord jobRecord = fetchJobRecord(operationId);
                if (jobRecord == null)
                {
                    continue;
                }

                List<List<UUID>> nodeExecutionOrder = jobRecord.nodeExecutionOrder();
                if (nodeExecutionOrder == null || nodeExecutionOrder.isEmpty())
                {
                    continue;
                }

                // nodeStatuses is a snapshot from storage. Local mutations (e.g., during crash recovery)
                // may make it partially stale, but this is conservative — finalization requires all nodes
                // terminal, so stale CREATED entries only delay finalization by one poll cycle.
                Map<UUID, OperationalJobStatus> nodeStatuses = storageProvider.getNodeStatusesForOperation(operationId);

                if (jobRecord.status().isUnsuccessful())
                {
                    abandonOperation(operationId, jobRecord, nodeExecutionOrder, nodeStatuses);
                }
                else
                {
                    trySubmitJobsForLocalInstances(operationId, operationType, jobRecord, nodeExecutionOrder,
                                                   nodeStatuses);
                }

                checkAndFinalizeJobIfComplete(operationId, operationType, jobRecord, nodeStatuses);
            }

            promise.complete();
        }
        catch (Exception e)
        {
            LOGGER.error("Error during local job coordination poll", e);
            promise.complete();
        }
    }

    private void trySubmitJobsForLocalInstances(UUID operationId,
                                                 OperationType operationType,
                                                 OperationalJobRecord jobRecord,
                                                 List<List<UUID>> nodeExecutionOrder,
                                                 Map<UUID, OperationalJobStatus> nodeStatuses)
    {
        for (InstanceMetadata instance : instanceMetadataFetcher.allLocalInstances())
        {
            UUID hostId = localHostId(instance);
            if (hostId == null)
            {
                continue;
            }
            String host = instance.host();

            int groupIndex = findGroupIndex(hostId, nodeExecutionOrder);
            if (groupIndex < 0)
            {
                continue;
            }

            OperationalJobStatus currentStatus = nodeStatuses.get(hostId);

            // Node has already reached a terminal state — nothing to do
            if (currentStatus != null && currentStatus.isCompleted())
            {
                continue;
            }

            // Crash recovery: node is RUNNING but we have no local job handle for this operation. This path
            // deliberately bypasses the settle gate below; see isGroupSettleElapsed for why
            if (currentStatus == OperationalJobStatus.RUNNING && localJobManager.getJob(operationId, hostId) == null)
            {
                handleCrashRecovery(operationId, operationType, hostId, host, nodeExecutionOrder, nodeStatuses,
                                    jobRecord.operationMetadata());
                continue;
            }

            // A handle already tracked for this operation+node means the job is in flight — nothing to do
            if (localJobManager.getJob(operationId, hostId) != null)
            {
                continue;
            }

            boolean requiresPredecessorSuccess = localJobFactory.requiresPredecessorSuccess();
            if (!arePredecessorsComplete(hostId, nodeExecutionOrder, nodeStatuses, requiresPredecessorSuccess))
            {
                OperationalJobStatus cascaded = requiresPredecessorSuccess
                                                ? cascadedPredecessorStatus(hostId, nodeExecutionOrder, nodeStatuses)
                                                : null;
                if (cascaded != null)
                {
                    LOGGER.warn("Predecessor did not succeed and operation requires predecessor success — marking " +
                                "node {} as {}. operationId={}", hostId, cascaded, operationId);
                    reportNodeStatusWithRetry(operationId, hostId, cascaded, 1);
                }
                // Else, an immediate predecessor is still running, so wait for the next poll. The wait is
                // inherently bounded because every RUNNING node has its own execution timeout (see submitLocalJob).
                continue;
            }

            if (!isGroupSettleElapsed(operationId, groupIndex, jobRecord.operationMetadata()))
            {
                continue;
            }

            submitLocalJob(operationId, operationType, hostId, host, jobRecord.operationMetadata());
        }
    }

    /**
     * Whether the settle window between the previous execution group and {@code groupIndex} has elapsed, giving the
     * nodes just operated on time to become genuinely ready before the next group starts.
     *
     * <p>The window runs from when <em>this</em> Sidecar first observed the predecessors complete rather than from
     * when they actually completed, which is not recorded. Observation can only lag completion, so the real pause is
     * never shorter than the configured wait. Because submission decisions are only made on a poll tick, the pause is
     * also rounded up to a whole number of {@code local_job_coordination_delay} intervals. A Sidecar restart mid-window
     * loses the observation and restarts the window.</p>
     *
     * <p>After a Sidecar crash, the nodes of one group can end up staggered rather than starting together. A node
     * left {@code RUNNING} is resubmitted by {@link #handleCrashRecovery} without consulting this gate. It had already
     * passed the gate when it was first submitted, so the window separating it from the previous group has long since
     * elapsed. A group-mate still {@code CREATED} has lost its observation along with the crash and therefore serves a
     * fresh full poll window. This is intentional: nodes share a group only when it is safe to operate on them at the
     * same time, so running them one after another is strictly more conservative than the parallelism the group permits.
     *
     * @return {@code true} if the next group may start, {@code false} to re-check on a later poll
     */
    private boolean isGroupSettleElapsed(UUID operationId, int groupIndex,
                                         Map<String, String> operationMetadata)
    {
        if (groupIndex == 0)
        {
            return true;
        }

        SecondBoundConfiguration wait = localJobFactory.waitBetweenExecutionGroups(operationMetadata);
        if (wait == null || wait.toMillis() <= 0)
        {
            return true;
        }

        long nowNanos = timeProvider.nanoTime();
        long observedAtNanos = predecessorsCompletedAtNanos.computeIfAbsent(new GroupKey(operationId, groupIndex),
                                                                           key -> nowNanos);
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(nowNanos - observedAtNanos);
        if (elapsedMs >= wait.toMillis())
        {
            return true;
        }

        // Reported in seconds to match the unit the window is configured and overridden in. The remaining time is
        // rounded up so a poll landing just inside the deadline does not report "0s more".
        long remainingSeconds = (long) Math.ceil((wait.toMillis() - elapsedMs) / 1000.0);
        LOGGER.info("Waiting {}s more before starting execution group {} — {}s of the {}s settle window has "
                    + "elapsed. operationId={}", remainingSeconds, groupIndex,
                    TimeUnit.MILLISECONDS.toSeconds(elapsedMs), wait.toSeconds(), operationId);
        return false;
    }

    private void handleCrashRecovery(UUID operationId, OperationType operationType,
                                      UUID hostId, String host,
                                      List<List<UUID>> nodeExecutionOrder,
                                      Map<UUID, OperationalJobStatus> nodeStatuses,
                                      Map<String, String> operationMetadata)
    {
        if (localJobFactory.canRecover())
        {
            LOGGER.info("Node {} can recover — resetting to CREATED and resubmitting. operationId={}",
                         hostId, operationId);
            tryReportNodeStatus(operationId, hostId, OperationalJobStatus.CREATED);
            nodeStatuses.put(hostId, OperationalJobStatus.CREATED);
            submitLocalJob(operationId, operationType, hostId, host, operationMetadata);
        }
        else
        {
            LOGGER.warn("Node {} cannot recover from RUNNING state — marking as FAILED. operationId={}",
                         hostId, operationId);
            reportNodeStatusWithRetry(operationId, hostId, OperationalJobStatus.FAILED, 1);
            LocalJob job = localJobFactory.createJob(operationId, hostId, host, operationType, operationMetadata);
            recoverFailedNode(job, operationId, hostId);
        }
    }

    private void submitLocalJob(UUID operationId, OperationType operationType, UUID hostId, String host,
                                Map<String, String> operationMetadata)
    {
        LocalJob localJob = localJobFactory.createJob(operationId, hostId, host, operationType, operationMetadata);
        tryReportNodeStatus(operationId, hostId, OperationalJobStatus.RUNNING);

        // Bound every RUNNING node so a hung job can't wedge the operation. Whichever of execution completes or
        // timeout fires happens first reports the terminal status, the other is a no-op via terminalReported to avoid
        // a race condition between the two.
        // Failing jobs when Sidecar is permanently down will be handled by (https://issues.apache.org/jira/browse/CASSSIDECAR-485).
        AtomicBoolean terminalReported = new AtomicBoolean(false);
        // Whether the terminal row was actually reported to storage
        AtomicBoolean terminalPersisted = new AtomicBoolean(false);
        long timeoutMs = resolveNodeExecutionTimeoutMs(operationType);
        long timerId = executorPools.internal().setTimer(timeoutMs, id -> {
            if (terminalReported.compareAndSet(false, true))
            {
                LOGGER.warn("Node {} exceeded execution timeout of {}ms — marking as FAILED and cancelling the job. "
                            + "operationId={}", hostId, timeoutMs, operationId);
                terminalPersisted.set(reportNodeStatusWithRetry(operationId, hostId,
                                                                OperationalJobStatus.FAILED, 1));
                // Stop the job from continuing after the operation has given up on it. The job is not interrupted;
                // it stops at its next isCancelled() check, and recovery then runs on the completion callback below.
                localJob.cancel();
            }
        });

        localJobManager.submitJob(localJob).onComplete(ar -> {
            executorPools.internal().cancelTimer(timerId);
            if (terminalReported.compareAndSet(false, true))
            {
                boolean failed = ar.failed();
                terminalPersisted.set(reportNodeStatusWithRetry(operationId, hostId,
                                                                failed ? OperationalJobStatus.FAILED
                                                                       : OperationalJobStatus.SUCCEEDED, 1));
                if (failed)
                {
                    LOGGER.error("Local job failed for node {}. operationId={}", hostId, operationId, ar.cause());
                    recoverFailedNode(localJob, operationId, hostId);
                }
            }
            else
            {
                // The timeout already fired and reported FAILED; the job has now stopped, so recover the node.
                recoverFailedNode(localJob, operationId, hostId);
            }

            // Keep the local job handle until the status is persisted so that the poll loop can retry 
            if (terminalPersisted.get())
            {
                localJobManager.removeJob(operationId, hostId, localJob);
            }
        });
    }

    /**
     * Runs the job's own recovery on its FAILED transition. Invoked only after the job has stopped. Failures
     * are logged and swallowed so recovery can never wedge the poll loop.
     *
     * <p>Dispatched to a worker thread so that {@link LocalJob#onJobFailed()} is free to block.</p>
     *
     * @return the outcome of running the recovery, which callers on the poll path ignore
     */
    @VisibleForTesting
    Future<Void> recoverFailedNode(LocalJob localJob, UUID operationId, UUID hostId)
    {
        return executorPools.internal()
                            .executeBlocking(localJob::onJobFailed, false)
                            // executeBlocking does not flatten, so without this the result is the future recovery
                            // returned rather than the outcome of running it, and a failure would go unreported.
                            .compose(recovery -> recovery)
                            .onFailure(e -> LOGGER.error("onJobFailed recovery failed for node {}. operationId={}",
                                                         hostId, operationId, e));
    }

    private long resolveNodeExecutionTimeoutMs(OperationType operationType)
    {
        SecondBoundConfiguration override = localJobFactory.nodeExecutionTimeout();
        return override != null ? override.toMillis() : defaultNodeExecutionTimeoutMs;
    }

    /**
     * Reports a node status, retrying with backoff on failure. Use this for terminal statuses
     * (SUCCEEDED/FAILED).
     *
     * <p>A terminal write is the one status write the poll loop cannot re-drive on its own. Once a node is
     * terminal (or its local job handle has completed) later polls short-circuit and never re-report it, so a
     * lost terminal write would keep the node in RUNNING.
     * See {@link #tryReportNodeStatus} for why non-terminal statuses are handled differently.
     *
     * @return whether the write to storage was successful
     */
    private boolean reportNodeStatusWithRetry(UUID operationId, UUID nodeId,
                                              OperationalJobStatus status, int attempt)
    {
        try
        {
            reportNodeStatus(operationId, nodeId, status);
            return true;
        }
        catch (Exception e)
        {
            LOGGER.warn("Failed to report status {} for node {} (attempt {}/{}). operationId={}",
                        status, nodeId, attempt, MAX_NODE_STATUS_RETRIES, operationId, e);
            if (attempt < MAX_NODE_STATUS_RETRIES)
            {
                executorPools.internal().setTimer(NODE_STATUS_RETRY_DELAY_MS * attempt,
                                                  id -> reportNodeStatusWithRetry(operationId, nodeId, status, attempt + 1));
            }
            else
            {
                LOGGER.error("Exhausted retries reporting status {} for node {}. operationId={}. " +
                             "Manual intervention may be required.", status, nodeId, operationId);
            }
            return false;
        }
    }

    @Override
    public boolean arePredecessorsComplete(UUID nodeId,
                                            List<List<UUID>> nodeExecutionOrder,
                                            Map<UUID, OperationalJobStatus> nodeStatuses,
                                            boolean requiresPredecessorSuccess)
    {
        int nodeGroupIndex = findGroupIndex(nodeId, nodeExecutionOrder);
        if (nodeGroupIndex < 0)
        {
            LOGGER.error("Node {} not found in execution order — cannot determine predecessor status", nodeId);
            return false;
        }
        if (nodeGroupIndex == 0)
        {
            return true;
        }

        // Only the immediate predecessor group needs checking. A node job reaches a terminal state during execution
        // (its own execution timeout guarantees it eventually does) or by cascade from a failed predecessor.
        // Both conditionos require its predecessors to already be terminal. As a result, an immediate predecessor
        // group that is complete implies the entire upstream predecessor chain is complete.
        for (UUID predecessorId : nodeExecutionOrder.get(nodeGroupIndex - 1))
        {
            OperationalJobStatus status = nodeStatuses.get(predecessorId);
            if (status == null)
            {
                LOGGER.error("Predecessor node {} has null status — expected a status entry", predecessorId);
                return false;
            }
            if (!status.isCompleted())
            {
                return false;
            }
            if (requiresPredecessorSuccess && status.isUnsuccessful())
            {
                return false;
            }
        }
        return true;
    }

    @Override
    public void reportNodeStatus(UUID operationId, UUID nodeId, OperationalJobStatus status)
    {
        storageProvider.updateNodeStatus(operationId, nodeId, status);
    }

    @Override
    public void checkAndFinalizeJobIfComplete(UUID operationId,
                                               OperationType operationType,
                                               OperationalJobRecord jobRecord,
                                               Map<UUID, OperationalJobStatus> nodeStatuses)
    {
        List<List<UUID>> nodeExecutionOrder = jobRecord.nodeExecutionOrder();
        if (nodeExecutionOrder == null)
        {
            return;
        }

        boolean anyUnsuccessful = nodeStatuses.values().stream().anyMatch(OperationalJobStatus::isUnsuccessful);
        boolean allTerminal = true;

        for (List<UUID> group : nodeExecutionOrder)
        {
            for (UUID nodeId : group)
            {
                OperationalJobStatus status = nodeStatuses.get(nodeId);
                if (status == null)
                {
                    if (anyUnsuccessful)
                    {
                        LOGGER.warn("Node {} has null status but other nodes did not succeed — treating as FAILED. " +
                                    "operationId={}", nodeId, operationId);
                        continue;
                    }
                    LOGGER.error("Node {} has null status during finalization check — expected a status entry", nodeId);
                    allTerminal = false;
                    break;
                }
                if (!status.isCompleted())
                {
                    allTerminal = false;
                    break;
                }
                if (status.isUnsuccessful())
                {
                    anyUnsuccessful = true;
                }
            }
            if (!allTerminal)
            {
                break;
            }
        }

        if (!allTerminal)
        {
            return;
        }

        // A record already in a terminal state carries an outcome that another Sidecar, or an operator aborting the
        // operation by hand, already recorded. Re-writing it would replace that reason with the generic one below,
        // and would downgrade an ABORTED record to FAILED.
        boolean recordAlreadyTerminal = jobRecord.status().isCompleted();
        OperationalJobStatus overallStatus = anyUnsuccessful ? OperationalJobStatus.FAILED
                                                             : OperationalJobStatus.SUCCEEDED;
        if (!recordAlreadyTerminal)
        {
            String failureReason = anyUnsuccessful ? "One or more nodes failed during operation" : null;
            storageProvider.updateJobStatus(operationId, operationType, overallStatus, failureReason);
        }
        // Clear lock held in the local datacenter (null). Active ops are discovered for the local DC in
        // getActiveOperations(), so a finalizer only ever sees operations living in its own local-DC.
        // Therefore, the local DC always equals the target DC the lock was set under.
        operationalJobCoordinator.clearActive(operationType, operationId, null);
        LOGGER.info("Job finalized. operationId={} operationType={} status={}", operationId, operationType,
                    recordAlreadyTerminal ? jobRecord.status() : overallStatus);
        cleanupOperationState(operationId);
    }

    /**
     * Reports a node status and logs on failure without retry. Use this for non-terminal statuses
     * (CREATED/RUNNING), which the poll loop re-derives from storage each on each poll (self-healing).
     * Retrying these would risk a delayed write landing after the terminal write and regressing the
     * status.
     * See {@link #reportNodeStatusWithRetry} for why terminal statuses are handled differently.
     */
    private void tryReportNodeStatus(UUID operationId, UUID nodeId, OperationalJobStatus status)
    {
        try
        {
            reportNodeStatus(operationId, nodeId, status);
        }
        catch (Exception e)
        {
            LOGGER.error("Failed to report status {} for node {}. operationId={}", status, nodeId, operationId, e);
        }
    }

    /**
     * Reads the job record from storage on every poll rather than caching it for the life of the operation.
     * The record carries the operation's authoritative status, so a job aborted by an operator, or a finalization 
     * performed by another Sidecar, has to be observable here.
     */
    private OperationalJobRecord fetchJobRecord(UUID operationId)
    {
        return storageProvider.findJob(operationId);
    }

    private int findGroupIndex(UUID nodeId, List<List<UUID>> nodeExecutionOrder)
    {
        for (int i = 0; i < nodeExecutionOrder.size(); i++)
        {
            if (nodeExecutionOrder.get(i).contains(nodeId))
            {
                return i;
            }
        }
        return -1;
    }

    /**
     * The terminal status to settle {@code nodeId} with because an immediate predecessor did not succeed and the
     * operation requires predecessor success. The predecessor's own status is propagated, so a node that never ran
     * because an operator aborted the operation records {@code ABORTED} rather than a failure it did not have.
     *
     * @return the status to cascade, or {@code null} if every immediate predecessor succeeded
     */
    @Nullable
    private OperationalJobStatus cascadedPredecessorStatus(UUID nodeId,
                                                           List<List<UUID>> nodeExecutionOrder,
                                                           Map<UUID, OperationalJobStatus> nodeStatuses)
    {
        int nodeGroupIndex = findGroupIndex(nodeId, nodeExecutionOrder);
        if (nodeGroupIndex <= 0)
        {
            return null;
        }
        for (UUID predecessorId : nodeExecutionOrder.get(nodeGroupIndex - 1))
        {
            // A predecessor with no status row yet is unknown, not unsuccessful, so it cascades nothing
            OperationalJobStatus status = nodeStatuses.get(predecessorId);
            if (status != null && status.isUnsuccessful())
            {
                return status;
            }
        }
        return null;
    }

    /**
     * Identifies one execution group within one operation, so settle windows for concurrent operations and for
     * successive groups of the same operation are tracked independently.
     */
    private static final class GroupKey
    {
        private final UUID operationId;
        private final int groupIndex;

        private GroupKey(UUID operationId, int groupIndex)
        {
            this.operationId = operationId;
            this.groupIndex = groupIndex;
        }

        @Override
        public boolean equals(Object other)
        {
            if (this == other)
            {
                return true;
            }
            if (!(other instanceof GroupKey))
            {
                return false;
            }
            GroupKey that = (GroupKey) other;
            return groupIndex == that.groupIndex && operationId.equals(that.operationId);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(operationId, groupIndex);
        }
    }

    /**
     * Cancels every local job for the operation. The job's completion callback drops the handle 
     * once the terminal status has been reported to storage.
     */
    private void cleanupJobsForOperation(UUID operationId)
    {
        for (LocalJob job : localJobManager.activeJobs())
        {
            if (operationId.equals(job.operationId()))
            {
                job.cancel();
            }
        }
    }

    /**
     * Cancels local jobs left over from operations that no longer hold an active-operation lock.
     *
     * @param activeOperationIds the operations that currently hold a lock in this datacenter
     */
    private void abandonOperationsWithoutLock(Collection<UUID> activeOperationIds)
    {
        Set<UUID> abandoned = new HashSet<>();
        for (LocalJob job : localJobManager.activeJobs())
        {
            if (!activeOperationIds.contains(job.operationId()))
            {
                abandoned.add(job.operationId());
            }
        }
        for (UUID operationId : abandoned)
        {
            LOGGER.warn("Operation no longer holds an active-operation lock; cancelling the local jobs still "
                        + "running for it and releasing its local state. operationId={}", operationId);
            cleanupOperationState(operationId);
        }
    }

    /**
     * Stops this Sidecar continuing an operation whose record has reached a terminal status, and settles the node
     * rows this Sidecar owns.
     *
     * <p>A node whose local job is still stopping keeps its row non-terminal until the job reports through its own
     * completion callback. This is what holds the operation's lock: finalization needs every row terminal, so the
     * lock cannot be released while any Sidecar still has work in flight. Only rows for this Sidecar's own
     * instances are written, so every row has exactly one writer.</p>
     */
    private void abandonOperation(UUID operationId, OperationalJobRecord jobRecord,
                                  List<List<UUID>> nodeExecutionOrder,
                                  Map<UUID, OperationalJobStatus> nodeStatuses)
    {
        boolean hasLocalJobs = localJobManager.activeJobs()
                                              .stream()
                                              .anyMatch(job -> operationId.equals(job.operationId()));
        if (hasLocalJobs)
        {
            LOGGER.warn("Operation is recorded as {}; cancelling any local job still running for it and "
                        + "releasing its local state. operationId={}", jobRecord.status(), operationId);
        }
        cleanupOperationState(operationId);
        settleLocalNodes(operationId, jobRecord, nodeExecutionOrder, nodeStatuses);
    }

    /**
     * Settles every local instance in the execution order that has not reached a terminal status and has no local
     * job left to report one.
     *
     * <p>{@link NodeSettlement} decides the status each node records. A node abandoned mid-execution is also
     * reconciled through {@link #settleNodeAbandonedMidExecution}.</p>
     */
    private void settleLocalNodes(UUID operationId, OperationalJobRecord jobRecord,
                                  List<List<UUID>> nodeExecutionOrder,
                                  Map<UUID, OperationalJobStatus> nodeStatuses)
    {
        for (InstanceMetadata instance : instanceMetadataFetcher.allLocalInstances())
        {
            UUID hostId = localHostId(instance);
            if (hostId == null || findGroupIndex(hostId, nodeExecutionOrder) < 0)
            {
                continue;
            }

            OperationalJobStatus currentStatus = nodeStatuses.get(hostId);
            NodeSettlement settlement = NodeSettlement.of(currentStatus);
            if (settlement == NodeSettlement.ALREADY_SETTLED)
            {
                continue;
            }

            // Check for presence of handle, not status. A job reaches a terminal status before its completion
            // callback writes the status to storage
            LocalJob localJob = localJobManager.getJob(operationId, hostId);
            if (localJob != null)
            {
                if (!localJob.status().isCompleted())
                {
                    LOGGER.info("Node {} still has a local job waiting to report status; leaving its status at {} "
                                + "until the job reports. operationId={}", hostId, currentStatus, operationId);
                    continue;
                }
                dischargeFinishedJobRow(operationId, hostId, localJob, nodeStatuses);
                continue;
            }

            if (settlement == NodeSettlement.ABANDONED_MID_EXECUTION)
            {
                settleNodeAbandonedMidExecution(operationId, jobRecord, hostId, instance.host(), nodeStatuses);
                continue;
            }

            OperationalJobStatus settled = settlement.terminalStatus(jobRecord.status());
            LOGGER.warn("Settling node {} as {}; the operation was abandoned before this node started executing. "
                        + "operationId={}", hostId, settled, operationId);
            if (reportNodeStatusWithRetry(operationId, hostId, settled, 1))
            {
                nodeStatuses.put(hostId, settled);
            }
        }
    }

    /**
     * Writes the terminal status a local job still owes, and releases its handle once that lands.
     *
     * <p>The job's completion callback normally does this. It leaves the handle behind when its write did not
     * reach storage, and nothing re-drives it: the retries inside {@link #reportNodeStatusWithRetry} are finite,
     * and {@link #resetState} only sweeps handles once no operation is active, which cannot happen while this
     * node keeps the operation from finalizing. The poll is what breaks that cycle.</p>
     *
     * <p>The job's own status is written rather than a settled one, because this Sidecar ran the work and knows
     * how it ended; settlement would only be guessing at it.</p>
     */
    private void dischargeFinishedJobRow(UUID operationId, UUID hostId, LocalJob localJob,
                                         Map<UUID, OperationalJobStatus> nodeStatuses)
    {
        OperationalJobStatus actual = localJob.status();
        LOGGER.warn("Node {} has a finished local job whose status never reached storage; reporting {} and "
                    + "releasing the job. operationId={}", hostId, actual, operationId);
        if (reportNodeStatusWithRetry(operationId, hostId, actual, 1))
        {
            nodeStatuses.put(hostId, actual);
            localJobManager.removeJob(operationId, hostId, localJob);
        }
    }

    /**
     * Settles a node that was executing but has no local job, which happens when the Sidecar process that started
     * it crashes.
     */
    private void settleNodeAbandonedMidExecution(UUID operationId, OperationalJobRecord jobRecord, UUID hostId,
                                                 String host, Map<UUID, OperationalJobStatus> nodeStatuses)
    {
        OperationalJobStatus settled = NodeSettlement.ABANDONED_MID_EXECUTION.terminalStatus(jobRecord.status());
        LOGGER.warn("Node {} was executing but has no local job; the Sidecar that started it is gone. Marking it "
                    + "{} and running recovery. operationId={}", hostId, settled, operationId);
        if (reportNodeStatusWithRetry(operationId, hostId, settled, 1))
        {
            nodeStatuses.put(hostId, settled);
        }

        // Recovery runs whether or not the report landed. The node needs reconciling either way, and a later poll
        // can try to re-report
        OperationType operationType = jobRecord.operationType();
        LocalJob localJob = localJobFactory.createJob(operationId, hostId, host, operationType,
                                                      jobRecord.operationMetadata());
        recoverFailedNode(localJob, operationId, hostId);
    }

    /**
     * @return the host ID of a local Cassandra instance, or {@code null} if its delegate cannot be reached
     */
    @Nullable
    private UUID localHostId(InstanceMetadata instance)
    {
        try
        {
            return instance.delegate().nodeSettings().hostId();
        }
        catch (Exception e)
        {
            LOGGER.debug("Skipping instance {} — delegate unavailable", instance.host(), e);
            return null;
        }
    }

    /**
     * Clears the local state for a single operation. Scoped to {@code operationId} so finalizing one operation
     * does not disturb others that are still active.
     */
    private void cleanupOperationState(UUID operationId)
    {
        predecessorsCompletedAtNanos.keySet().removeIf(key -> key.operationId.equals(operationId));
        cleanupJobsForOperation(operationId);
    }

    /**
     * Clears all local state. Only safe when no operations are active (see the {@code activeOps.isEmpty()}
     * branch); use {@link #cleanupOperationState} to tear down a single finalized operation.
     */
    private void resetState()
    {
        predecessorsCompletedAtNanos.clear();
        for (LocalJob job : localJobManager.activeJobs())
        {
            job.cancel();
            localJobManager.removeJob(job.operationId(), job.nodeId(), job);
        }
    }
}
