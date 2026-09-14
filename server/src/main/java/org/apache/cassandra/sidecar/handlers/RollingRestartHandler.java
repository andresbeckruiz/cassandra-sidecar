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

package org.apache.cassandra.sidecar.handlers;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.datastax.driver.core.utils.UUIDs;
import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.Json;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.acl.authorization.BasicPermissions;
import org.apache.cassandra.sidecar.common.request.data.RollingRestartRequestPayload;
import org.apache.cassandra.sidecar.common.response.RingResponse;
import org.apache.cassandra.sidecar.common.response.data.RingEntry;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.RollingRestartConfiguration;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.job.OperationalJobManager;
import org.apache.cassandra.sidecar.job.restart.NetworkTopologyExecutionOrderComputer;
import org.apache.cassandra.sidecar.job.restart.ReplicaScope;
import org.apache.cassandra.sidecar.job.restart.ReplicationStrategyAnalyzer;
import org.apache.cassandra.sidecar.job.restart.RestartOperationMetadata;
import org.apache.cassandra.sidecar.job.restart.RollingRestartJob;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.cassandra.sidecar.utils.OperationalJobUtils;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * Handles POST requests to {@code /api/v1/cassandra/operations/restart} to create a rolling restart job.
 * Validates the request, resolves cluster topology, computes execution order, and submits the job.
 */
public class RollingRestartHandler extends AbstractHandler<RollingRestartRequestPayload> implements AccessProtected
{
    static final int MAX_WAIT_BETWEEN_EXECUTION_GROUPS_SECONDS = 30 * 60;

    private final OperationalJobManager jobManager;
    private final ServiceConfiguration config;
    private final RollingRestartConfiguration rollingRestartConfig;
    private final ReplicationStrategyAnalyzer strategyAnalyzer;

    @Inject
    protected RollingRestartHandler(InstanceMetadataFetcher metadataFetcher,
                                    ExecutorPools executorPools,
                                    ServiceConfiguration serviceConfiguration,
                                    CassandraInputValidator validator,
                                    OperationalJobManager jobManager,
                                    RollingRestartConfiguration rollingRestartConfig,
                                    ReplicationStrategyAnalyzer strategyAnalyzer)
    {
        super(metadataFetcher, executorPools, validator);
        this.jobManager = jobManager;
        this.config = serviceConfiguration;
        this.rollingRestartConfig = rollingRestartConfig;
        this.strategyAnalyzer = strategyAnalyzer;
    }

    @Override
    public Set<Authorization> requiredAuthorizations()
    {
        return Collections.singleton(BasicPermissions.RESTART.toAuthorization());
    }

    @Override
    protected RollingRestartRequestPayload extractParamsOrThrow(RoutingContext context)
    {
        if (!rollingRestartConfig.enabled())
        {
            throw wrapHttpException(HttpResponseStatus.SERVICE_UNAVAILABLE,
                                    "Rolling restart feature is not enabled");
        }

        RollingRestartRequestPayload payload;
        try
        {
            payload = Json.decodeValue(context.body().asString(), RollingRestartRequestPayload.class);
        }
        catch (DecodeException | IllegalArgumentException e)
        {
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST, "Invalid request body: " + e.getMessage());
        }

        String datacenter = payload.datacenter();
        if (datacenter == null || datacenter.isBlank())
        {
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST, "datacenter is required");
        }

        if (payload.maxRestartParallelism() < 1)
        {
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST,
                                    "maxRestartParallelism must be at least 1");
        }

        // Optional per-request overrides, when supplied, must be within range. Absent (null) overrides fall back to
        // the bounded configuration defaults and need no validation. The timeouts must be positive; the retry count
        // counts retries beyond the first attempt, so zero (restart once, no retry) is valid but negatives are not.
        // Zero is likewise valid for the inter-group wait, where it means "do not wait", but that field also carries
        // an upper bound because nothing downstream limits an oversized settle window.
        validatePositiveOverride(payload.cassandraHealthTimeoutSeconds(), "cassandraHealthTimeoutSeconds");
        validatePositiveOverride(payload.nodeStateTransitionTimeoutSeconds(), "nodeStateTransitionTimeoutSeconds");
        validateNonNegativeOverride(payload.nodeRestartRetryAttempts(), "nodeRestartRetryAttempts");
        validateBoundedOverride(payload.waitBetweenExecutionGroupsSeconds(), "waitBetweenExecutionGroupsSeconds",
                                MAX_WAIT_BETWEEN_EXECUTION_GROUPS_SECONDS);

        return payload;
    }

    /**
     * Validates that an optional numeric override, when provided, is greater than zero. A {@code null} value means
     * the override was omitted and the configuration default applies, so it passes validation.
     */
    private static void validatePositiveOverride(Integer value, String fieldName)
    {
        if (value != null && value < 1)
        {
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST, fieldName + " must be greater than 0");
        }
    }

    /**
     * Validates that an optional count-or-duration override, when provided, is not negative. A {@code null} value
     * means the override was omitted and the configuration default applies. Zero is valid for these fields: for the
     * retry count it means restart once with no retry, and for the inter-group wait it means do not wait.
     */
    private static void validateNonNegativeOverride(Integer value, String fieldName)
    {
        if (value != null && value < 0)
        {
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST, fieldName + " must not be negative");
        }
    }

    /**
     * Validates that an optional override, when provided, is neither negative nor greater than {@code maxInclusive}.
     * A {@code null} value means the override was omitted and the configuration default applies.
     */
    private static void validateBoundedOverride(Integer value, String fieldName, int maxInclusive)
    {
        validateNonNegativeOverride(value, fieldName);
        if (value != null && value > maxInclusive)
        {
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST, fieldName + " must not exceed " + maxInclusive);
        }
    }

    @Override
    protected void handleInternal(RoutingContext context,
                                  HttpServerRequest httpRequest,
                                  @NotNull String host,
                                  SocketAddress remoteAddress,
                                  RollingRestartRequestPayload payload)
    {
        // Resolving the ring and computing the execution order require blocking Cassandra calls, so this work runs
        // off the event loop. Validation failures thrown here surface as HTTP errors through processFailure.
        executorPools.service()
                     .executeBlocking(() -> buildJob(host, payload))
                     .onSuccess(job -> jobManager.trySubmitJob(
                         job,
                         (completedJob, exception) ->
                         OperationalJobUtils.sendStatusBasedResponse(context, completedJob, exception),
                         executorPools.service(),
                         config.operationalJobExecutionMaxWaitTime()))
                     .onFailure(cause -> processFailure(cause, context, host, remoteAddress, payload));
    }

    /**
     * Resolves the cluster topology from the ring, validates the requested nodes against the target datacenter,
     * and computes the rack-aware execution order for the job. Runs on a blocking worker thread.
     */
    private RollingRestartJob buildJob(String host, RollingRestartRequestPayload payload)
    {
        StorageOperations operations = metadataFetcher.delegate(host).storageOperations();
        RingResponse ring;
        try
        {
            ring = operations.ring(null);
        }
        catch (UnknownHostException e)
        {
            throw wrapHttpException(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Failed to retrieve ring topology", e);
        }

        List<UUID> targetNodes = resolveTargetNodes(ring, payload);

        int safeParallelism = resolveSafeParallelism(ring, payload);

        List<List<UUID>> executionOrder = new NetworkTopologyExecutionOrderComputer(safeParallelism)
                                          .computeOrder(ring, targetNodes);

        // The effective parallelism is how many nodes are actually restarted together at peak, or the size of the
        // largest execution group. It can be lower than the requested (and safe) upper bound when a rack holds
        // fewer nodes than the bound.
        int effectiveParallelism = executionOrder.stream().mapToInt(List::size).max().orElse(0);
        logger.info("Rolling restart for datacenter {}: requested maxRestartParallelism={}, effective "
                    + "maxRestartParallelism={} across {} execution group(s)",
                    payload.datacenter(), payload.maxRestartParallelism(), effectiveParallelism, executionOrder.size());

        return new RollingRestartJob(UUIDs.timeBased(),
                                     executionOrder,
                                     buildOperationMetadata(payload, effectiveParallelism),
                                     payload.datacenter());
    }

    /**
     * Resolves the safe upper bound on restart parallelism to feed the execution-order computer.
     * {@code maxRestartParallelism} is an upper bound, not a requirement: parallel restarts group same-rack nodes,
     * which is only safe when nodes in a rack do not share data replicas. When the datacenter's replication strategy
     * cannot guarantee that, the bound is capped to 1 (serial restarts) rather than rejecting the request.
     */
    private int resolveSafeParallelism(RingResponse ring, RollingRestartRequestPayload payload)
    {
        int requested = payload.maxRestartParallelism();
        // Values below 1 are rejected in extractParamsOrThrow, so a request of exactly 1 is already serial.
        if (requested == 1)
        {
            return requested;
        }

        ReplicaScope scope = strategyAnalyzer.replicaScopeForDatacenter(ring, payload.datacenter());
        if (scope == ReplicaScope.RACK)
        {
            return requested;
        }

        logger.info("Requested maxRestartParallelism={} is not safe for datacenter {} under its replication strategy "
                    + "(replica scope {}: same-rack nodes may share a replica); restarts will run serially",
                    requested, payload.datacenter(), scope);
        return 1;
    }

    /**
     * Resolves the nodes to restart within the target datacenter. When the request omits the node list <em>or</em>
     * sends an empty one, every node in the datacenter is targeted; otherwise each requested node id is validated
     * for format, datacenter membership, and uniqueness.
     */
    private List<UUID> resolveTargetNodes(RingResponse ring, RollingRestartRequestPayload payload)
    {
        String datacenter = payload.datacenter();
        Map<UUID, String> rackByNode = new HashMap<>();
        for (RingEntry entry : ring)
        {
            // datacenter() is validated non-null in extractParamsOrThrow, so compare from the payload side to
            // tolerate ring entries with a null datacenter.
            if (!datacenter.equals(entry.datacenter()))
            {
                continue;
            }
            UUID nodeId = parseHostId(entry.hostId());
            if (nodeId == null)
            {
                logger.warn("Skipping ring entry in datacenter {} with missing or malformed host id: {}",
                            datacenter, entry.hostId());
                continue;
            }
            rackByNode.put(nodeId, entry.rack());
        }

        if (rackByNode.isEmpty())
        {
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST, "Unknown datacenter: " + datacenter);
        }

        if (payload.nodes().isEmpty())
        {
            return new ArrayList<>(rackByNode.keySet());
        }

        List<UUID> targetNodes = new ArrayList<>(payload.nodes().size());
        Set<UUID> seen = new HashSet<>();
        for (String nodeIdStr : payload.nodes())
        {
            UUID nodeId = parseHostId(nodeIdStr);
            if (nodeId == null)
            {
                throw wrapHttpException(HttpResponseStatus.BAD_REQUEST, "Invalid node UUID: " + nodeIdStr);
            }
            if (!rackByNode.containsKey(nodeId))
            {
                throw wrapHttpException(HttpResponseStatus.BAD_REQUEST,
                                        "Node " + nodeId + " is not in datacenter " + datacenter);
            }
            if (!seen.add(nodeId))
            {
                throw wrapHttpException(HttpResponseStatus.BAD_REQUEST, "Duplicate node id: " + nodeId);
            }
            targetNodes.add(nodeId);
        }
        return targetNodes;
    }

    /**
     * Parses a host id into a {@link UUID}, returning {@code null} when the value is missing or malformed.
     */
    private UUID parseHostId(String hostId)
    {
        if (hostId == null)
        {
            return null;
        }
        try
        {
            return UUID.fromString(hostId);
        }
        catch (IllegalArgumentException e)
        {
            return null;
        }
    }

    private Map<String, String> buildOperationMetadata(RollingRestartRequestPayload payload, int effectiveParallelism)
    {
        Map<String, String> operationMetadata = new HashMap<>();
        if (payload.cassandraHealthTimeoutSeconds() != null)
        {
            operationMetadata.put(RestartOperationMetadata.CASSANDRA_HEALTH_TIMEOUT_SECONDS,
                                  String.valueOf(payload.cassandraHealthTimeoutSeconds()));
        }
        if (payload.nodeStateTransitionTimeoutSeconds() != null)
        {
            operationMetadata.put(RestartOperationMetadata.NODE_STATE_TRANSITION_TIMEOUT_SECONDS,
                                  String.valueOf(payload.nodeStateTransitionTimeoutSeconds()));
        }
        if (payload.nodeRestartRetryAttempts() != null)
        {
            operationMetadata.put(RestartOperationMetadata.NODE_RESTART_RETRY_ATTEMPTS,
                                  String.valueOf(payload.nodeRestartRetryAttempts()));
        }
        if (payload.waitBetweenExecutionGroupsSeconds() != null)
        {
            operationMetadata.put(RestartOperationMetadata.WAIT_BETWEEN_EXECUTION_GROUPS_SECONDS,
                                  String.valueOf(payload.waitBetweenExecutionGroupsSeconds()));
        }
        operationMetadata.put(RestartOperationMetadata.EFFECTIVE_MAX_RESTART_PARALLELISM,
                              String.valueOf(effectiveParallelism));
        return operationMetadata;
    }
}
