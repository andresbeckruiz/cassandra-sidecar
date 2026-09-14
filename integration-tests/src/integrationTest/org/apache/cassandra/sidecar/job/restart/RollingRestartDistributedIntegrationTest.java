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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.utils.UUIDs;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.shared.AbstractBuilder;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.common.data.OperationType;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.apache.cassandra.sidecar.common.response.NodeSettings;
import org.apache.cassandra.sidecar.common.server.utils.DriverUtils;
import org.apache.cassandra.sidecar.common.server.utils.GossipInfoParser;
import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.yaml.OperationalJobConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.RollingRestartConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SchemaKeyspaceConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SidecarConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.TestServiceConfiguration;
import org.apache.cassandra.sidecar.job.OperationalJobCoordinator;
import org.apache.cassandra.sidecar.job.storage.OperationalJobRecord;
import org.apache.cassandra.sidecar.job.storage.StorageProvider;
import org.apache.cassandra.sidecar.testing.SharedClusterSidecarIntegrationTestBase;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;

import static org.apache.cassandra.distributed.shared.NetworkTopology.dcAndRack;
import static org.apache.cassandra.distributed.shared.NetworkTopology.networkTopology;
import static org.apache.cassandra.sidecar.testing.SharedClusterIntegrationTestBase.IntegrationTestModule.cassandraInstanceHostname;
import static org.apache.cassandra.testing.utils.AssertionUtils.loopAssert;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Distributed integration test for rolling restart coordination across multiple Sidecar instances.
 * Validates the full coordination flow: job creation in storage, activation, local job discovery,
 * predecessor ordering, restart execution, and finalization.
 */
class RollingRestartDistributedIntegrationTest extends SharedClusterSidecarIntegrationTestBase
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RollingRestartDistributedIntegrationTest.class);

    private static final int SETTLE_WINDOW_SECONDS = 45;
    // How long the successor is asserted to stay CREATED. Well under SETTLE_WINDOW_SECONDS so observation lag cannot
    // make a correct implementation fail.
    private static final int SETTLE_CHECK_SECONDS = 25;
    private static final long SETTLE_POLL_INTERVAL_MS = 2000;

    private final Map<String, ServerWrapper> sidecarServerMap = new HashMap<>();
    private final DriverUtils driverUtils = new DriverUtils();

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        ClusterBuilderConfiguration config = super.testClusterConfiguration().nodesPerDc(3);
        config.clusterBuilderUpdater = (AbstractBuilder<?, ?, ?> clusterBuilder) ->
            clusterBuilder.withNodeIdTopology(networkTopology(3,
                nodeId -> dcAndRack("datacenter1", "rack" + nodeId)));
        return config;
    }

    @Override
    protected Function<SidecarConfigurationImpl.Builder, SidecarConfigurationImpl.Builder> configurationOverrides()
    {
        return builder -> builder
            .serviceConfiguration(TestServiceConfiguration.builder()
                                                          .schemaKeyspaceConfiguration(
                                                              SchemaKeyspaceConfigurationImpl.builder()
                                                                                             .isEnabled(true)
                                                                                             .replicationStrategy("NetworkTopologyStrategy")
                                                                                             .replicationFactor(3)
                                                                                             .build())
                                                          .build())
            .operationalJobConfiguration(
                OperationalJobConfigurationImpl.builder()
                                               .coordinationEnabled(true)
                                               .durableTrackingEnabled(true)
                                               .localJobCoordinationDelay(SecondBoundConfiguration.parse("5s"))
                                               .nodeExecutionTimeout(SecondBoundConfiguration.parse("10m"))
                                               .rollingRestartConfiguration(
                                                   RollingRestartConfigurationImpl.builder()
                                                                                  .enabled(true)
                                                                                  .cassandraHealthTimeout(SecondBoundConfiguration.parse("30s"))
                                                                                  .nodeStateTransitionTimeout(SecondBoundConfiguration.parse("120s"))
                                                                                  .nodeRestartRetryAttempts(3)
                                                                                  .waitBetweenExecutionGroups(SecondBoundConfiguration.parse("0s"))
                                                                                  .build())
                                               .build());
    }

    @Override
    protected void startSidecar(ICluster<? extends IInstance> cluster) throws InterruptedException
    {
        for (IInstance cassandraInstance : cluster)
        {
            LOGGER.info("Starting Sidecar instance for Cassandra instance {}",
                        cassandraInstance.config().num());
            String cassandraInstanceHostname = cassandraInstanceHostname(cassandraInstance, dnsResolver);
            ServerWrapper serverWrapper = startSidecarWithInstances(List.of(cassandraInstance));
            sidecarServerMap.put(cassandraInstanceHostname, serverWrapper);

            if (this.serverWrapper == null)
            {
                this.serverWrapper = serverWrapper;
            }
        }
    }

    @Override
    protected void initializeSchemaForTest()
    {
    }

    @Override
    protected void stopSidecar()
    {
        sidecarServerMap.values().forEach(sw -> {
            try
            {
                closeServer(sw);
            }
            catch (Exception e)
            {
                LOGGER.error("Error trying to close sidecar server", e);
            }
        });
    }

    @Test
    void testDistributedRestartSequentialExecution() throws InterruptedException
    {
        prepareStorage();

        ServerWrapper firstSidecar = sidecarServerMap.values().iterator().next();
        StorageProvider storageProvider = firstSidecar.injector.getInstance(StorageProvider.class);
        Map<UUID, ServerWrapper> sidecarByNodeId = resolveNodeIdToSidecar();
        UUID[] nodeIds = sidecarByNodeId.keySet().toArray(new UUID[0]);
        UUID operationId = UUIDs.timeBased();

        // Capture each node's gossip generation before the restart so we can later prove the restart occurred.
        Map<UUID, Long> generationBefore = readGossipGenerations(sidecarByNodeId, Arrays.asList(nodeIds));

        // Create a restart job with sequential execution order (one node per group)
        List<List<UUID>> executionOrder = Arrays.asList(
            List.of(nodeIds[0]),
            List.of(nodeIds[1]),
            List.of(nodeIds[2])
        );

        OperationalJobRecord jobRecord = OperationalJobRecord.builder()
                                                             .jobId(operationId)
                                                             .operationType(OperationType.RESTART)
                                                             .status(OperationalJobStatus.RUNNING)
                                                             .nodeExecutionOrder(executionOrder)
                                                             .build();
        storageProvider.persistJob(jobRecord);
        storageProvider.trySetActiveOperation(OperationType.RESTART, operationId, null);

        for (UUID nodeId : nodeIds)
        {
            storageProvider.updateNodeStatus(operationId, nodeId, OperationalJobStatus.CREATED);
        }

        LOGGER.info("Job {} persisted with execution order: {}", operationId, executionOrder);

        // The StorageBackedLocalJobCoordinator on each Sidecar polls every 5s (configured above).
        // Wait for all nodes to complete their restarts. RF=3 ensures reads succeed during restarts.
        // Wrap storage calls in try-catch because loopAssert only retries on AssertionError,
        // and transient ReadTimeoutExceptions can occur while a node is restarting.
        loopAssert(300, () -> {
            for (UUID nodeId : nodeIds)
            {
                OperationalJobStatus status;
                try
                {
                    status = storageProvider.getNodeStatus(operationId, nodeId);
                }
                catch (Exception e)
                {
                    LOGGER.warn("Transient error reading node status for {}, will retry", nodeId, e);
                    throw new AssertionError("Transient storage error, will retry: " + e.getMessage(), e);
                }
                assertThat(status)
                .describedAs("Node %s should eventually succeed, current status: %s", nodeId, status)
                .isEqualTo(OperationalJobStatus.SUCCEEDED);
            }
        });

        // Prove each node actually restarted rather than just having its status flipped to SUCCEEDED.
        assertGossipGenerationsIncreased(sidecarByNodeId, generationBefore);

        // Verify the job is finalized as SUCCEEDED
        loopAssert(60, () -> {
            OperationalJobRecord finalRecord;
            try
            {
                finalRecord = storageProvider.findJob(operationId);
            }
            catch (Exception e)
            {
                LOGGER.warn("Transient error reading job record, will retry", e);
                throw new AssertionError("Transient storage error, will retry: " + e.getMessage(), e);
            }
            assertThat(finalRecord).isNotNull();
            assertThat(finalRecord.status())
            .describedAs("Job should be finalized as SUCCEEDED")
            .isEqualTo(OperationalJobStatus.SUCCEEDED);
        });

        // Verify active operation is cleared (may lag behind job finalization)
        OperationalJobCoordinator coordinator = firstSidecar.injector.getInstance(OperationalJobCoordinator.class);
        loopAssert(60, () -> {
            assertThat(coordinator.getActiveOperation(OperationType.RESTART))
            .describedAs("Active operation should be cleared")
            .isNull();
        });
    }

    @Test
    void testNodeFailureBlocksSubsequentGroup() throws InterruptedException
    {
        prepareStorage();
        ServerWrapper firstSidecar = sidecarServerMap.values().iterator().next();
        StorageProvider storageProvider = firstSidecar.injector.getInstance(StorageProvider.class);
        UUID[] nodeIds = resolveNodeIds();
        UUID operationId = UUIDs.timeBased();

        // Sequential order; the first node is pre-seeded as FAILED to simulate a failed restart.
        List<List<UUID>> executionOrder = Arrays.asList(
            List.of(nodeIds[0]),
            List.of(nodeIds[1]),
            List.of(nodeIds[2])
        );

        OperationalJobRecord jobRecord = OperationalJobRecord.builder()
                                                             .jobId(operationId)
                                                             .operationType(OperationType.RESTART)
                                                             .status(OperationalJobStatus.RUNNING)
                                                             .nodeExecutionOrder(executionOrder)
                                                             .build();
        storageProvider.persistJob(jobRecord);
        storageProvider.trySetActiveOperation(OperationType.RESTART, operationId, null);
        storageProvider.updateNodeStatus(operationId, nodeIds[0], OperationalJobStatus.FAILED);
        storageProvider.updateNodeStatus(operationId, nodeIds[1], OperationalJobStatus.CREATED);
        storageProvider.updateNodeStatus(operationId, nodeIds[2], OperationalJobStatus.CREATED);

        LOGGER.info("Job {} persisted with node {} pre-failed", operationId, nodeIds[0]);

        // RESTART blocks on node failure: subsequent groups must not run. Nodes 1 and 2 should be marked
        // FAILED (never SUCCEEDED) and the job must finalize as FAILED.
        loopAssert(120, () -> {
            OperationalJobRecord finalRecord;
            Map<UUID, OperationalJobStatus> statuses;
            try
            {
                finalRecord = storageProvider.findJob(operationId);
                statuses = storageProvider.getNodeStatusesForOperation(operationId);
            }
            catch (Exception e)
            {
                LOGGER.warn("Transient storage error, will retry", e);
                throw new AssertionError("Transient storage error, will retry: " + e.getMessage(), e);
            }
            assertThat(finalRecord).isNotNull();
            assertThat(finalRecord.status())
            .describedAs("Job should finalize as FAILED when a node fails")
            .isEqualTo(OperationalJobStatus.FAILED);
            assertThat(statuses.get(nodeIds[1]))
            .describedAs("Successor node 1 must not run past a failed predecessor")
            .isEqualTo(OperationalJobStatus.FAILED);
            assertThat(statuses.get(nodeIds[2]))
            .describedAs("Successor node 2 must not run past a failed predecessor")
            .isEqualTo(OperationalJobStatus.FAILED);
        });

        OperationalJobCoordinator coordinator = firstSidecar.injector.getInstance(OperationalJobCoordinator.class);
        loopAssert(60, () -> assertThat(coordinator.getActiveOperation(OperationType.RESTART))
                             .describedAs("Active operation should be cleared")
                             .isNull());
    }

    @Test
    void testSettleWindowHoldsBackSubsequentGroup() throws InterruptedException
    {
        prepareStorage();
        ServerWrapper firstSidecar = sidecarServerMap.values().iterator().next();
        StorageProvider storageProvider = firstSidecar.injector.getInstance(StorageProvider.class);
        Map<UUID, ServerWrapper> sidecarByNodeId = resolveNodeIdToSidecar();
        UUID[] nodeIds = sidecarByNodeId.keySet().toArray(new UUID[0]);
        UUID operationId = UUIDs.timeBased();

        Map<UUID, Long> generationBefore = readGossipGenerations(sidecarByNodeId,
                                                                 Arrays.asList(nodeIds[0], nodeIds[1]));

        // Two groups, so the operation spans exactly one settle window.
        List<List<UUID>> executionOrder = Arrays.asList(List.of(nodeIds[0]), List.of(nodeIds[1]));
        Map<String, String> operationMetadata =
            Map.of(RestartOperationMetadata.WAIT_BETWEEN_EXECUTION_GROUPS_SECONDS,
                   String.valueOf(SETTLE_WINDOW_SECONDS));

        OperationalJobRecord jobRecord = OperationalJobRecord.builder()
                                                             .jobId(operationId)
                                                             .operationType(OperationType.RESTART)
                                                             .status(OperationalJobStatus.RUNNING)
                                                             .nodeExecutionOrder(executionOrder)
                                                             .operationMetadata(operationMetadata)
                                                             .build();
        storageProvider.persistJob(jobRecord);
        storageProvider.trySetActiveOperation(OperationType.RESTART, operationId, null);
        storageProvider.updateNodeStatus(operationId, nodeIds[0], OperationalJobStatus.CREATED);
        storageProvider.updateNodeStatus(operationId, nodeIds[1], OperationalJobStatus.CREATED);

        LOGGER.info("Job {} persisted with a {}s settle window between groups",
                    operationId, SETTLE_WINDOW_SECONDS);

        loopAssert(180, () -> {
            OperationalJobStatus status;
            try
            {
                status = storageProvider.getNodeStatus(operationId, nodeIds[0]);
            }
            catch (Exception e)
            {
                throw new AssertionError("Transient storage error, will retry: " + e.getMessage(), e);
            }
            assertThat(status)
            .describedAs("First group should restart without waiting, current status: %s", status)
            .isEqualTo(OperationalJobStatus.SUCCEEDED);
        });
        long firstGroupObservedAtNanos = System.nanoTime();

        // The successor must stay CREATED while the window is open. Detecting the first group's completion can lag
        // its actual completion by a poll interval, and the coordinator measures the window from its own observation,
        // so both effects push the real subsequent execution time later. Checking over a window shorter than the
        // configured settle window keeps that slack from turning a correct wait into a test failure.
        long checkUntilNanos = firstGroupObservedAtNanos + TimeUnit.SECONDS.toNanos(SETTLE_CHECK_SECONDS);
        while (System.nanoTime() < checkUntilNanos)
        {
            OperationalJobStatus successorStatus;
            try
            {
                successorStatus = storageProvider.getNodeStatus(operationId, nodeIds[1]);
            }
            catch (Exception e)
            {
                LOGGER.warn("Transient error reading successor status, continuing to poll", e);
                Thread.sleep(SETTLE_POLL_INTERVAL_MS);
                continue;
            }
            assertThat(successorStatus)
            .describedAs("Successor must not start until the %ss settle window elapses", SETTLE_WINDOW_SECONDS)
            .isEqualTo(OperationalJobStatus.CREATED);
            Thread.sleep(SETTLE_POLL_INTERVAL_MS);
        }

        // Held back, not blocked: the successor must still run once the window closes.
        loopAssert(180, () -> {
            OperationalJobStatus status;
            try
            {
                status = storageProvider.getNodeStatus(operationId, nodeIds[1]);
            }
            catch (Exception e)
            {
                throw new AssertionError("Transient storage error, will retry: " + e.getMessage(), e);
            }
            assertThat(status)
            .describedAs("Successor should run once the settle window elapses, current status: %s", status)
            .isEqualTo(OperationalJobStatus.SUCCEEDED);
        });

        assertGossipGenerationsIncreased(sidecarByNodeId, generationBefore);

        OperationalJobCoordinator coordinator = firstSidecar.injector.getInstance(OperationalJobCoordinator.class);
        loopAssert(60, () -> assertThat(coordinator.getActiveOperation(OperationType.RESTART))
                             .describedAs("Active operation should be cleared")
                             .isNull());
    }

    @Test
    void testCrashRecoveryResubmitsJob() throws InterruptedException
    {
        prepareStorage();
        ServerWrapper firstSidecar = sidecarServerMap.values().iterator().next();
        StorageProvider storageProvider = firstSidecar.injector.getInstance(StorageProvider.class);
        Map<UUID, ServerWrapper> sidecarByNodeId = resolveNodeIdToSidecar();
        UUID[] nodeIds = sidecarByNodeId.keySet().toArray(new UUID[0]);
        UUID operationId = UUIDs.timeBased();

        Map<UUID, Long> generationBefore = readGossipGenerations(sidecarByNodeId, List.of(nodeIds[0]));

        // Single-node operation; the node is seeded RUNNING with no local job handle, simulating a Sidecar
        // that crashed mid-restart and lost its in-memory job on restart. The coordinator must detect the
        // orphaned RUNNING node (canRecover() == true), reset it, and resubmit the job.
        List<List<UUID>> executionOrder = List.of(List.of(nodeIds[0]));
        OperationalJobRecord jobRecord = OperationalJobRecord.builder()
                                                             .jobId(operationId)
                                                             .operationType(OperationType.RESTART)
                                                             .status(OperationalJobStatus.RUNNING)
                                                             .nodeExecutionOrder(executionOrder)
                                                             .build();
        storageProvider.persistJob(jobRecord);
        storageProvider.trySetActiveOperation(OperationType.RESTART, operationId, null);
        storageProvider.updateNodeStatus(operationId, nodeIds[0], OperationalJobStatus.RUNNING);

        LOGGER.info("Job {} persisted with orphaned RUNNING node {}", operationId, nodeIds[0]);

        loopAssert(300, () -> {
            OperationalJobStatus status;
            try
            {
                status = storageProvider.getNodeStatus(operationId, nodeIds[0]);
            }
            catch (Exception e)
            {
                LOGGER.warn("Transient storage error, will retry", e);
                throw new AssertionError("Transient storage error, will retry: " + e.getMessage(), e);
            }
            assertThat(status)
            .describedAs("Recovered node should eventually succeed, current status: %s", status)
            .isEqualTo(OperationalJobStatus.SUCCEEDED);
        });

        // Prove each node actually restarted rather than just having its status flipped to SUCCEEDED.
        assertGossipGenerationsIncreased(sidecarByNodeId, generationBefore);

        loopAssert(60, () -> {
            OperationalJobRecord finalRecord;
            try
            {
                finalRecord = storageProvider.findJob(operationId);
            }
            catch (Exception e)
            {
                LOGGER.warn("Transient storage error, will retry", e);
                throw new AssertionError("Transient storage error, will retry: " + e.getMessage(), e);
            }
            assertThat(finalRecord).isNotNull();
            assertThat(finalRecord.status())
            .describedAs("Recovered job should finalize as SUCCEEDED")
            .isEqualTo(OperationalJobStatus.SUCCEEDED);
        });
    }

    private void prepareStorage() throws InterruptedException
    {
        for (ServerWrapper sw : sidecarServerMap.values())
        {
            waitForSchemaReady(sw, 60, TimeUnit.SECONDS);
        }
        // A prior test's restart may leave the CQL session briefly degraded; retry initialize until it succeeds.
        loopAssert(120, () -> {
            for (ServerWrapper sw : sidecarServerMap.values())
            {
                try
                {
                    sw.injector.getInstance(StorageProvider.class).initialize();
                }
                catch (Exception e)
                {
                    throw new AssertionError("StorageProvider not ready: " + e.getMessage(), e);
                }
            }
        });
    }

    private UUID[] resolveNodeIds()
    {
        return resolveNodeIdToSidecar().keySet().toArray(new UUID[0]);
    }

    /**
     * Maps each node's host ID to the Sidecar co-located with it. Gossip generation must be read from a node's
     * own Sidecar (peer views lag by gossip propagation), so callers verifying restarts need this association.
     */
    private Map<UUID, ServerWrapper> resolveNodeIdToSidecar()
    {
        Map<UUID, ServerWrapper> sidecarByNodeId = new HashMap<>();
        // When this runs after a test that restarted nodes, a Sidecar's Cassandra delegate may still be
        // reconnecting (delegate() throws CassandraUnavailableException). Retry until all are available.
        loopAssert(120, () -> {
            sidecarByNodeId.clear();
            for (Map.Entry<String, ServerWrapper> entry : sidecarServerMap.entrySet())
            {
                InstanceMetadataFetcher fetcher = entry.getValue().injector.getInstance(InstanceMetadataFetcher.class);
                try
                {
                    NodeSettings settings = fetcher.allLocalInstances().get(0).delegate().nodeSettings();
                    sidecarByNodeId.put(settings.hostId(), entry.getValue());
                }
                catch (Exception e)
                {
                    throw new AssertionError("Delegate not yet available for " + entry.getKey() + ": " + e.getMessage(), e);
                }
            }
        });
        return sidecarByNodeId;
    }

    /**
     * Reads the gossip generation for {@code hostId} from its own Sidecar. 
     */
    private long readGossipGeneration(ServerWrapper sw, UUID hostId)
    {
        CassandraAdapterDelegate delegate =
            sw.injector.getInstance(InstanceMetadataFetcher.class).allLocalInstances().get(0).delegate();
        return GossipInfoParser.parse(delegate.clusterMembershipOperations().gossipInfo())
                               .values().stream()
                               .filter(i -> hostId.toString().equalsIgnoreCase(i.hostId()))
                               .map(i -> Long.parseLong(i.generation()))
                               .findFirst()
                               .orElseThrow(() -> new AssertionError("No gossip entry for " + hostId));
    }

    /**
     * Reads each given node's current gossip generation from its own Sidecar.
     */
    private Map<UUID, Long> readGossipGenerations(Map<UUID, ServerWrapper> sidecarByNodeId,
                                                  Collection<UUID> nodeIds)
    {
        Map<UUID, Long> generations = new HashMap<>();
        for (UUID nodeId : nodeIds)
        {
            generations.put(nodeId, readGossipGeneration(sidecarByNodeId.get(nodeId), nodeId));
        }
        return generations;
    }

    /**
     * Asserts every node's gossip generation strictly increased from its pre-restart value, proving a real
     * restart happened. Queries each node's own Sidecar and retries to tolerate transient reads and the 
     * brief window before a rebooted node re-gossips its new generation.
     */
    private void assertGossipGenerationsIncreased(Map<UUID, ServerWrapper> sidecarByNodeId,
                                                  Map<UUID, Long> generationBefore)
    {
        loopAssert(60, () -> {
            for (Map.Entry<UUID, Long> before : generationBefore.entrySet())
            {
                UUID nodeId = before.getKey();
                long generationAfter;
                try
                {
                    generationAfter = readGossipGeneration(sidecarByNodeId.get(nodeId), nodeId);
                }
                catch (Exception e)
                {
                    LOGGER.warn("Transient error reading gossip generation for {}, will retry", nodeId, e);
                    throw new AssertionError("Transient gossip read error, will retry: " + e.getMessage(), e);
                }
                assertThat(generationAfter)
                .describedAs("Node %s gossip generation must increase after a real restart (before=%s)",
                             nodeId, before.getValue())
                .isGreaterThan(before.getValue());
            }
        });
    }
}
