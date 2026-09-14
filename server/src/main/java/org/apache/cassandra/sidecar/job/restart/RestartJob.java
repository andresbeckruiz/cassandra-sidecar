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

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Future;
import org.apache.cassandra.sidecar.common.data.Lifecycle.CassandraState;
import org.apache.cassandra.sidecar.common.data.Lifecycle.OperationStatus;
import org.apache.cassandra.sidecar.common.data.OperationType;
import org.apache.cassandra.sidecar.common.response.LifecycleInfoResponse;
import org.apache.cassandra.sidecar.common.response.RingResponse;
import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;
import org.apache.cassandra.sidecar.exceptions.LifecycleTaskConflictException;
import org.apache.cassandra.sidecar.job.LocalJob;
import org.apache.cassandra.sidecar.lifecycle.LifecycleManager;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Per-node job that performs a Cassandra restart with ring health checks and retry logic.
 * Runs on a worker thread — all methods may block.
 */
public class RestartJob extends LocalJob
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RestartJob.class);
    static final long DEFAULT_HEALTH_CHECK_POLL_INTERVAL_MS = TimeUnit.SECONDS.toMillis(15);
    static final long DEFAULT_LIFECYCLE_POLL_INTERVAL_MS = TimeUnit.SECONDS.toMillis(5);
    private static final long RETRY_BASE_DELAY_MS = TimeUnit.SECONDS.toMillis(5);
    private static final long RETRY_JITTER_MS = TimeUnit.SECONDS.toMillis(2);

    private final LifecycleManager lifecycleManager;
    private final RestartHealthChecker healthChecker;
    private final int nodeRestartRetryAttempts;
    private final SecondBoundConfiguration cassandraHealthTimeout;
    // Bounds a single node lifecycle state transition (one stop or one start), not the whole restart. A restart
    // attempt performs up to two transitions, so its worst case is ~2x this plus cassandraHealthTimeout, and the
    // job may make up to (nodeRestartRetryAttempts + 1) attempts.
    private final SecondBoundConfiguration nodeStateTransitionTimeout;
    private final RingSupplier ringSupplier;
    private long healthCheckPollIntervalMs = DEFAULT_HEALTH_CHECK_POLL_INTERVAL_MS;
    private long lifecyclePollIntervalMs = DEFAULT_LIFECYCLE_POLL_INTERVAL_MS;

    /**
     * Functional interface for supplying ring data, decoupling from StorageOperations.
     */
    @FunctionalInterface
    public interface RingSupplier
    {
        RingResponse getRing();
    }

    public RestartJob(UUID operationId, UUID nodeId, String instanceHost, OperationType operationType,
                      LifecycleManager lifecycleManager,
                      RestartHealthChecker healthChecker,
                      int nodeRestartRetryAttempts,
                      SecondBoundConfiguration cassandraHealthTimeout,
                      SecondBoundConfiguration nodeStateTransitionTimeout,
                      RingSupplier ringSupplier)
    {
        super(operationId, nodeId, instanceHost, operationType);
        this.lifecycleManager = lifecycleManager;
        this.healthChecker = healthChecker;
        this.nodeRestartRetryAttempts = nodeRestartRetryAttempts;
        this.cassandraHealthTimeout = cassandraHealthTimeout;
        this.nodeStateTransitionTimeout = nodeStateTransitionTimeout;
        this.ringSupplier = ringSupplier;
    }

    /**
     * Overrides the poll intervals so unit tests run quickly.
     */
    @VisibleForTesting
    void setPollIntervalsForTesting(long healthCheckPollIntervalMs, long lifecyclePollIntervalMs)
    {
        this.healthCheckPollIntervalMs = healthCheckPollIntervalMs;
        this.lifecyclePollIntervalMs = lifecyclePollIntervalMs;
    }

    @Override
    protected void executeInternal()
    {
        // nodeRestartRetryAttempts counts retries beyond the first attempt, so the total number of attempts is
        // retries + 1 (0 retries means a single attempt with no retry).
        int maxAttempts = nodeRestartRetryAttempts + 1;
        for (int attempt = 1; attempt <= maxAttempts; attempt++)
        {
            throwIfCancelled();
            LOGGER.info("Restart attempt {}/{} for node {}. operationId={}", attempt, maxAttempts, nodeId(), operationId());
            try
            {
                if (attemptRestart())
                {
                    LOGGER.info("Restart succeeded for node {}. operationId={}", nodeId(), operationId());
                    return;
                }
            }
            catch (RestartFailedException e)
            {
                throw e;
            }
            catch (Exception e)
            {
                LOGGER.warn("Restart attempt {}/{} failed unexpectedly. operationId={}", attempt, maxAttempts, operationId(), e);
            }

            if (attempt < maxAttempts)
            {
                sleep(retryBackoffMillis(attempt));
            }
        }
        throw new RestartFailedException("Restart failed after " + maxAttempts + " attempts");
    }

    /**
     * Performs a single restart attempt. If the node is running, the ring health is checked before stopping it
     * (stopping a live node could reduce availability); if it is already down, it is started directly. The
     * specific step that did not converge is logged at WARN.
     *
     * @return {@code true} if the restart converged, {@code false} if a step did not converge and the attempt
     *         should be retried
     */
    private boolean attemptRestart()
    {
        if (isNodeRunning())
        {
            if (!waitForRingHealth())
            {
                LOGGER.warn("Ring did not become healthy within the timeout. operationId={}", operationId());
                return false;
            }
            if (!updateLifecycleStateAndWait(CassandraState.STOPPED))
            {
                LOGGER.warn("Stop did not converge within the timeout. operationId={}", operationId());
                return false;
            }
        }
        else
        {
            // The node is already down: the ring health check is moot (starting an already-stopped node
            // cannot reduce availability), so skip the health check and stop and start it directly.
            LOGGER.info("Node {} is already down; skipping health check and stop, starting directly. operationId={}",
                        nodeId(), operationId());
        }

        if (!updateLifecycleStateAndWait(CassandraState.RUNNING))
        {
            LOGGER.warn("Start did not converge within the timeout. operationId={}", operationId());
            return false;
        }
        return true;
    }

    /**
     * Best-effort reconciliation invoked when the cluster-wide job terminates as FAILED: if this node was left
     * stopped, it attempts to start it back up. This is not guaranteed to succeed — any failure is logged and
     * swallowed, and the returned future always succeeds so it never blocks the coordinator's cleanup.
     */
    @Override
    public Future<Void> onJobFailed()
    {
        try
        {
            LifecycleInfoResponse info = lifecycleManager.getLifecycleInfo(instanceHost());
            if (!info.currentState().isRunning())
            {
                LOGGER.info("Node {} is not running after job failure; attempting to start. operationId={}",
                            nodeId(), operationId());
                lifecycleManager.updateDesiredState(instanceHost(), CassandraState.RUNNING);
            }
        }
        catch (LifecycleTaskConflictException e)
        {
            LOGGER.warn("Lifecycle task conflict during onJobFailed recovery for node {}. operationId={}",
                        nodeId(), operationId(), e);
        }
        catch (Exception e)
        {
            LOGGER.warn("Failed to restart node {} during onJobFailed recovery. operationId={}",
                        nodeId(), operationId(), e);
        }
        return Future.succeededFuture();
    }

    private boolean waitForRingHealth()
    {
        long deadline = System.currentTimeMillis() + cassandraHealthTimeout.toMillis();
        while (System.currentTimeMillis() < deadline)
        {
            if (isCancelled())
            {
                return false;
            }
            try
            {
                if (healthChecker.isHealthy(ringSupplier.getRing(), nodeId()))
                {
                    return true;
                }
            }
            catch (Exception e)
            {
                LOGGER.warn("Failed to query ring for health check. operationId={}", operationId(), e);
            }
            sleep(healthCheckPollIntervalMs);
        }
        return false;
    }

    /**
     * @return whether the local Cassandra node is currently running. On any error determining the state,
     * conservatively assumes the node is running so the ring health gate is still applied before a stop.
     */
    private boolean isNodeRunning()
    {
        try
        {
            return lifecycleManager.getLifecycleInfo(instanceHost()).currentState().isRunning();
        }
        catch (Exception e)
        {
            LOGGER.warn("Could not determine lifecycle state for node {}; assuming running. operationId={}",
                        nodeId(), operationId(), e);
            return true;
        }
    }

    /**
     * Requests a lifecycle state transition and waits for it to converge. Handles the case where
     * a previous transition's async cleanup hasn't completed yet by retrying the state request
     * within the overall timeout.
     *
     * @param desiredState the target state (STOPPED or RUNNING)
     * @return true if the transition converged within the timeout, false otherwise
     */
    private boolean updateLifecycleStateAndWait(CassandraState desiredState)
    {
        LOGGER.info("{} Cassandra on node {}. operationId={}",
                    desiredState.isRunning() ? "Starting" : "Stopping", nodeId(), operationId());

        long deadline = System.currentTimeMillis() + nodeStateTransitionTimeout.toMillis();
        boolean transitionRequested = false;

        while (System.currentTimeMillis() < deadline)
        {
            if (isCancelled())
            {
                return false;
            }
            if (!transitionRequested)
            {
                try
                {
                    lifecycleManager.updateDesiredState(instanceHost(), desiredState);
                    transitionRequested = true;
                }
                catch (LifecycleTaskConflictException e)
                {
                    // A previous transition's async cleanup may not have completed yet, so the lifecycle
                    // manager rejects the new request; retry within the overall timeout.
                    LOGGER.debug("Lifecycle task conflict, will retry. operationId={}", operationId());
                    sleep(lifecyclePollIntervalMs);
                    continue;
                }
            }

            try
            {
                LifecycleInfoResponse info = lifecycleManager.getLifecycleInfo(instanceHost());
                if (info.status() == OperationStatus.CONVERGED && info.currentState() == desiredState)
                {
                    return true;
                }
                if (info.status() == OperationStatus.DIVERGED)
                {
                    LOGGER.warn("Lifecycle diverged while waiting for {} on node {}. operationId={}",
                                desiredState, nodeId(), operationId());
                    return false;
                }
            }
            catch (Exception e)
            {
                LOGGER.warn("Failed to check lifecycle info. operationId={}", operationId(), e);
            }
            sleep(lifecyclePollIntervalMs);
        }
        return false;
    }

    /**
     * Backoff between retry attempts, with jitter so multiple Sidecars retrying against the same transient
     * condition do not retry in lockstep.
     */
    private long retryBackoffMillis(int attempt)
    {
        return RETRY_BASE_DELAY_MS * attempt + ThreadLocalRandom.current().nextLong(RETRY_JITTER_MS);
    }

    /**
     * Bails out of the restart if cancellation has been requested (e.g. the coordinator hit the per-node
     * execution timeout). Throwing here surfaces the job as FAILED and stops it from starting the next
     * lifecycle transition.
     */
    private void throwIfCancelled()
    {
        if (isCancelled())
        {
            throw new RestartFailedException("Restart cancelled for node " + nodeId() + ". operationId=" + operationId());
        }
    }

    private void sleep(long millis)
    {
        try
        {
            Thread.sleep(millis);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw new RestartFailedException("Restart interrupted", e);
        }
    }
}
