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

import java.net.UnknownHostException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.common.data.OperationType;
import org.apache.cassandra.sidecar.common.response.RingResponse;
import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.RollingRestartConfiguration;
import org.apache.cassandra.sidecar.job.LocalJob;
import org.apache.cassandra.sidecar.job.LocalJobFactory;
import org.apache.cassandra.sidecar.lifecycle.LifecycleManager;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Factory for creating {@link RestartJob} instances. Handles RESTART operation type only.
 */
@Singleton
public class RestartLocalJobFactory implements LocalJobFactory
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RestartLocalJobFactory.class);
    private static final long EXECUTION_TIMEOUT_MARGIN_SECONDS = 60;

    private final InstanceMetadataFetcher metadataFetcher;
    private final LifecycleManager lifecycleManager;
    private final RestartHealthChecker healthChecker;
    private final RollingRestartConfiguration config;

    @Inject
    public RestartLocalJobFactory(InstanceMetadataFetcher metadataFetcher,
                                 LifecycleManager lifecycleManager,
                                 RestartHealthChecker healthChecker,
                                 RollingRestartConfiguration config)
    {
        this.metadataFetcher = metadataFetcher;
        this.lifecycleManager = lifecycleManager;
        this.healthChecker = healthChecker;
        this.config = config;
    }

    @Override
    public LocalJob createJob(UUID operationId, UUID nodeId, String instanceHost, OperationType operationType)
    {
        return createJob(operationId, nodeId, instanceHost, operationType, null);
    }

    @Override
    public LocalJob createJob(UUID operationId, UUID nodeId, String instanceHost, OperationType operationType,
                             Map<String, String> operationMetadata)
    {
        if (operationType != OperationType.RESTART)
        {
            throw new UnsupportedOperationException("RestartLocalJobFactory only supports RESTART, got " + operationType);
        }

        LOGGER.info("Creating RestartJob for node {}. operationId={} instanceHost={}", nodeId, operationId, instanceHost);

        // Per-request overrides take precedence; any field the request omitted falls back to the
        // configured yaml defaults.
        int retryAttempts = resolveRetryAttempts(operationMetadata, config);
        SecondBoundConfiguration healthTimeout = resolveHealthTimeout(operationMetadata, config);
        SecondBoundConfiguration nodeStateTransitionTimeout = resolveNodeStateTransitionTimeout(operationMetadata, config);

        // Ring access is deferred to the supplier (rather than resolved here) so job construction never touches
        // the ring or the Cassandra delegate: failures surface inside the job's retry loop instead of aborting
        // the coordinator's reconcile tick.
        return new RestartJob(operationId, nodeId, instanceHost, operationType,
                              lifecycleManager, healthChecker,
                              retryAttempts, healthTimeout, nodeStateTransitionTimeout,
                              () -> ring(instanceHost));
    }

    @VisibleForTesting
    static int resolveRetryAttempts(Map<String, String> operationMetadata, RollingRestartConfiguration config)
    {
        Integer override = parseIntOrNull(operationMetadata, RestartOperationMetadata.NODE_RESTART_RETRY_ATTEMPTS);
        return override != null ? override : config.nodeRestartRetryAttempts();
    }

    @VisibleForTesting
    static SecondBoundConfiguration resolveHealthTimeout(Map<String, String> operationMetadata,
                                                         RollingRestartConfiguration config)
    {
        Integer seconds = parseIntOrNull(operationMetadata, RestartOperationMetadata.CASSANDRA_HEALTH_TIMEOUT_SECONDS);
        return seconds != null ? SecondBoundConfiguration.parse(seconds + "s") : config.cassandraHealthTimeout();
    }

    @VisibleForTesting
    static SecondBoundConfiguration resolveNodeStateTransitionTimeout(Map<String, String> operationMetadata,
                                                              RollingRestartConfiguration config)
    {
        Integer seconds = parseIntOrNull(operationMetadata, RestartOperationMetadata.NODE_STATE_TRANSITION_TIMEOUT_SECONDS);
        return seconds != null ? SecondBoundConfiguration.parse(seconds + "s") : config.nodeStateTransitionTimeout();
    }

    private static Integer parseIntOrNull(Map<String, String> operationMetadata, String key)
    {
        if (operationMetadata == null)
        {
            return null;
        }
        String raw = operationMetadata.get(key);
        if (raw == null)
        {
            return null;
        }
        try
        {
            return Integer.valueOf(raw);
        }
        catch (NumberFormatException e)
        {
            LOGGER.warn("Ignoring malformed operation metadata {}={}", key, raw);
            return null;
        }
    }

    /**
     * A restarted node can report up while its caches are still cold, so the next rack waits out this window before
     * it starts. A per-request override takes precedence over the configured default; zero opts the restart out.
     */
    @Override
    public SecondBoundConfiguration waitBetweenExecutionGroups(Map<String, String> operationMetadata)
    {
        Integer seconds = parseIntOrNull(operationMetadata, RestartOperationMetadata.WAIT_BETWEEN_EXECUTION_GROUPS_SECONDS);
        // A negative value cannot come from the request handler, which rejects it, so treat one read back from
        // storage the same as a malformed value and fall back to the configured default.
        if (seconds == null || seconds < 0)
        {
            return config.waitBetweenExecutionGroups();
        }
        return SecondBoundConfiguration.parse(seconds + "s");
    }

    @Override
    public boolean requiresPredecessorSuccess()
    {
        return true;
    }

    @Override
    public boolean canRecover()
    {
        return true;
    }

    /**
     * Bounds the coordinator's per-node timeout above the worst-case restart runtime rather than using the generic
     * {@code operational_job.node_execution_timeout}, which is too small for a restart. A restart attempt waits
     * for ring health, then performs two lifecycle transitions (stop and start), each up to its own timeout, and the
     * job retries up to {@code nodeRestartRetryAttempts + 1} times.
     */
    @Override
    public SecondBoundConfiguration nodeExecutionTimeout()
    {
        long worstCaseSeconds =
            (config.cassandraHealthTimeout().toSeconds() + config.nodeStateTransitionTimeout().toSeconds() * 2)
            * (config.nodeRestartRetryAttempts() + 1);
        return new SecondBoundConfiguration(worstCaseSeconds + EXECUTION_TIMEOUT_MARGIN_SECONDS, TimeUnit.SECONDS);
    }

    private RingResponse ring(String instanceHost)
    {
        try
        {
            return metadataFetcher.instance(instanceHost).delegate().storageOperations().ring(null);
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException("Failed to query ring for " + instanceHost, e);
        }
    }
}
