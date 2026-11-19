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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

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
 * Validates the full coordination flow, including job creation in storage, activation, local job
 * discovery, predecessor ordering, restart execution, and finalization.
 */
class RollingRestartDistributedIntegrationTest extends SharedClusterSidecarIntegrationTestBase
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RollingRestartDistributedIntegrationTest.class);

    private final Map<String, ServerWrapper> sidecarServerMap = new HashMap<>();
    private StorageProvider storageProvider;

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
            LOGGER.info("Starting Sidecar instance for Cassandra instance {}", cassandraInstance.config().num());
            ServerWrapper sidecar = startSidecarWithInstances(List.of(cassandraInstance));
            sidecarServerMap.put(cassandraInstanceHostname(cassandraInstance, dnsResolver), sidecar);
            if (serverWrapper == null)
            {
                serverWrapper = sidecar;
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
        Map<UUID, ServerWrapper> sidecarByNodeId = resolveNodeIdToSidecar();
        UUID[] nodeIds = sidecarByNodeId.keySet().toArray(new UUID[0]);
        Map<UUID, Long> generationBefore = readGossipGenerations(sidecarByNodeId, Arrays.asList(nodeIds));

        UUID operationId = persistOperation(Arrays.asList(List.of(nodeIds[0]), List.of(nodeIds[1]), List.of(nodeIds[2])),
                                            Map.of(nodeIds[0], OperationalJobStatus.CREATED,
                                                   nodeIds[1], OperationalJobStatus.CREATED,
                                                   nodeIds[2], OperationalJobStatus.CREATED));

        loopAssert(300, () -> {
            Map<UUID, OperationalJobStatus> statuses = readStorage(() -> storageProvider.getNodeStatusesForOperation(operationId));
            for (UUID nodeId : nodeIds)
            {
                assertThat(statuses.get(nodeId))
                .describedAs("Node %s should eventually succeed", nodeId)
                .isEqualTo(OperationalJobStatus.SUCCEEDED);
            }
        });
        assertGossipGenerationsIncreased(sidecarByNodeId, generationBefore);
        assertOperationFinalizes(operationId, OperationalJobStatus.SUCCEEDED);
        assertActiveOperationCleared();
    }

    @Test
    void testNodeFailureBlocksSubsequentGroup() throws InterruptedException
    {
        prepareStorage();
        UUID[] nodeIds = resolveNodeIdToSidecar().keySet().toArray(new UUID[0]);

        UUID operationId = persistOperation(Arrays.asList(List.of(nodeIds[0]), List.of(nodeIds[1]), List.of(nodeIds[2])),
                                            Map.of(nodeIds[0], OperationalJobStatus.FAILED,
                                                   nodeIds[1], OperationalJobStatus.CREATED,
                                                   nodeIds[2], OperationalJobStatus.CREATED));

        assertOperationFinalizes(operationId, OperationalJobStatus.FAILED);
        Map<UUID, OperationalJobStatus> statuses = readStorage(() -> storageProvider.getNodeStatusesForOperation(operationId));
        assertThat(statuses.get(nodeIds[1])).isEqualTo(OperationalJobStatus.FAILED);
        assertThat(statuses.get(nodeIds[2])).isEqualTo(OperationalJobStatus.FAILED);
        assertActiveOperationCleared();
    }

    @Test
    void testCrashRecoveryResubmitsJob() throws InterruptedException
    {
        prepareStorage();
        Map<UUID, ServerWrapper> sidecarByNodeId = resolveNodeIdToSidecar();
        UUID nodeId = sidecarByNodeId.keySet().iterator().next();
        Map<UUID, Long> generationBefore = readGossipGenerations(sidecarByNodeId, List.of(nodeId));

        // A node marked as RUNNING with no local job handle is what a Sidecar that crashed mid-restart leaves behind
        UUID operationId = persistOperation(List.of(List.of(nodeId)), Map.of(nodeId, OperationalJobStatus.RUNNING));

        assertOperationFinalizes(operationId, OperationalJobStatus.SUCCEEDED);
        assertThat(readStorage(() -> storageProvider.getNodeStatus(operationId, nodeId)))
        .isEqualTo(OperationalJobStatus.SUCCEEDED);
        assertGossipGenerationsIncreased(sidecarByNodeId, generationBefore);
        assertActiveOperationCleared();
    }

    private UUID persistOperation(List<List<UUID>> executionOrder, Map<UUID, OperationalJobStatus> nodeStatuses)
    {
        UUID operationId = UUIDs.timeBased();
        storageProvider.persistJob(OperationalJobRecord.builder()
                                                       .jobId(operationId)
                                                       .operationType(OperationType.RESTART)
                                                       .status(OperationalJobStatus.RUNNING)
                                                       .nodeExecutionOrder(executionOrder)
                                                       .build());
        assertThat(storageProvider.trySetActiveOperation(OperationType.RESTART, operationId)).isTrue();
        nodeStatuses.forEach((nodeId, status) -> storageProvider.updateNodeStatus(operationId, nodeId, status));
        LOGGER.info("Operation {} persisted with execution order {} and node statuses {}",
                    operationId, executionOrder, nodeStatuses);
        return operationId;
    }

    private void assertOperationFinalizes(UUID operationId, OperationalJobStatus expected)
    {
        loopAssert(300, () -> {
            OperationalJobRecord record = readStorage(() -> storageProvider.findJob(operationId));
            assertThat(record).isNotNull();
            assertThat(record.status()).isEqualTo(expected);
        });
    }

    private void assertActiveOperationCleared()
    {
        OperationalJobCoordinator coordinator = serverWrapper.injector.getInstance(OperationalJobCoordinator.class);
        loopAssert(60, () -> assertThat(readStorage(() -> coordinator.getActiveOperation(OperationType.RESTART)))
                             .describedAs("Active operation should be cleared")
                             .isNull());
    }

    private static <T> T readStorage(Supplier<T> read)
    {
        try
        {
            return read.get();
        }
        catch (Exception e)
        {
            throw new AssertionError("Transient storage error during restart, will retry: " + e.getMessage(), e);
        }
    }

    private void prepareStorage() throws InterruptedException
    {
        for (ServerWrapper sw : sidecarServerMap.values())
        {
            waitForSchemaReady(sw, 60, TimeUnit.SECONDS);
        }
        loopAssert(120, () -> {
            for (ServerWrapper sw : sidecarServerMap.values())
            {
                readStorage(() -> {
                    sw.injector.getInstance(StorageProvider.class).initialize();
                    return null;
                });
            }
        });
        storageProvider = serverWrapper.injector.getInstance(StorageProvider.class);
    }

    private Map<UUID, ServerWrapper> resolveNodeIdToSidecar()
    {
        Map<UUID, ServerWrapper> sidecarByNodeId = new HashMap<>();
        loopAssert(120, () -> {
            sidecarByNodeId.clear();
            for (ServerWrapper sw : sidecarServerMap.values())
            {
                NodeSettings settings = readStorage(() -> delegate(sw).nodeSettings());
                sidecarByNodeId.put(settings.hostId(), sw);
            }
        });
        return sidecarByNodeId;
    }

    private Map<UUID, Long> readGossipGenerations(Map<UUID, ServerWrapper> sidecarByNodeId, List<UUID> nodeIds)
    {
        Map<UUID, Long> generations = new HashMap<>();
        for (UUID nodeId : nodeIds)
        {
            generations.put(nodeId, readGossipGeneration(sidecarByNodeId.get(nodeId), nodeId));
        }
        return generations;
    }

    private void assertGossipGenerationsIncreased(Map<UUID, ServerWrapper> sidecarByNodeId,
                                                  Map<UUID, Long> generationBefore)
    {
        loopAssert(60, () -> generationBefore.forEach((nodeId, before) -> {
            long after = readStorage(() -> readGossipGeneration(sidecarByNodeId.get(nodeId), nodeId));
            assertThat(after)
            .describedAs("Node %s gossip generation must increase after a restart", nodeId)
            .isGreaterThan(before);
        }));
    }

    private static long readGossipGeneration(ServerWrapper sw, UUID hostId)
    {
        return GossipInfoParser.parse(delegate(sw).clusterMembershipOperations().gossipInfo())
                               .values().stream()
                               .filter(info -> hostId.toString().equalsIgnoreCase(info.hostId()))
                               .map(info -> Long.parseLong(info.generation()))
                               .findFirst()
                               .orElseThrow(() -> new AssertionError("No gossip entry for " + hostId));
    }

    private static CassandraAdapterDelegate delegate(ServerWrapper sw)
    {
        return sw.injector.getInstance(InstanceMetadataFetcher.class).allLocalInstances().get(0).delegate();
    }
}
