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

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.utils.UUIDs;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.data.OperationType;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.apache.cassandra.sidecar.common.response.NodeSettings;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.job.storage.OperationalJobRecord;
import org.apache.cassandra.sidecar.job.storage.StorageProvider;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.cassandra.sidecar.utils.TimeProvider;
import org.apache.cassandra.testing.CassandraIntegrationTest;

import static org.apache.cassandra.testing.utils.AssertionUtils.loopAssert;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Integration test for {@link StorageBackedLocalJobCoordinator} verifying real Cassandra-backed
 * storage interactions: node status updates, predecessor checking, and job finalization.
 */
class StorageBackedLocalJobCoordinatorIntegrationTest extends IntegrationTestBase
{
    private static final String OPERATOR_ABORT_REASON = "Aborted by operator request";

    @CassandraIntegrationTest
    void testCoordinatorPollCycleWithStorageProvider() throws InterruptedException
    {
        waitForSchemaReady(10, TimeUnit.SECONDS);

        StorageProvider storageProvider = injector.getInstance(StorageProvider.class);
        storageProvider.initialize();
        SidecarConfiguration sidecarConfig = injector.getInstance(SidecarConfiguration.class);

        UUID operationId = UUIDs.timeBased();
        UUID node1 = UUID.randomUUID();
        UUID node2 = UUID.randomUUID();

        OperationalJobRecord jobRecord = OperationalJobRecord.builder()
                                                             .jobId(operationId)
                                                             .operationType(OperationType.DRAIN)
                                                             .status(OperationalJobStatus.RUNNING)
                                                             .nodeExecutionOrder(Arrays.asList(
                                                             Arrays.asList(node1),
                                                             Arrays.asList(node2)
                                                             ))
                                                             .build();
        storageProvider.persistJob(jobRecord);
        storageProvider.trySetActiveOperation(OperationType.DRAIN, operationId, null);
        storageProvider.updateNodeStatus(operationId, node1, OperationalJobStatus.CREATED);
        storageProvider.updateNodeStatus(operationId, node2, OperationalJobStatus.CREATED);

        InstanceMetadataFetcher mockFetcher = mockFetcherForNode(node2, "127.0.0.2");

        LocalJobFactory testFactory = (opId, nodeId, host, opType) -> new LocalJob(opId, nodeId, host, opType)
        {
            @Override
            protected void executeInternal()
            {
            }
        };

        OperationalJobCoordinator opCoordinator = new StorageBackedOperationalJobCoordinator(storageProvider);
        LocalJobManager localJobManager = new LocalJobManager(injector.getInstance(
                                           org.apache.cassandra.sidecar.concurrent.ExecutorPools.class));

        StorageBackedLocalJobCoordinator coordinator = new StorageBackedLocalJobCoordinator(
                                                                                            injector.getInstance(org.apache.cassandra.sidecar.concurrent.ExecutorPools.class),
                                                                                            opCoordinator,
                                                                                            storageProvider,
                                                                                            mockFetcher,
                                                                                            localJobManager,
                                                                                            sidecarConfig,
                                                                                            testFactory,
                                                                                            TimeProvider.DEFAULT_TIME_PROVIDER);

        // Scenario 1: Node1 (predecessor) is still CREATED — node2 should not be submitted
        executeAndWait(coordinator);

        OperationalJobStatus node2Status = storageProvider.getNodeStatus(operationId, node2);
        assertThat(node2Status)
        .describedAs("Node2 should remain CREATED while predecessor is not complete")
        .isEqualTo(OperationalJobStatus.CREATED);

        // Scenario 2: Mark node1 as SUCCEEDED — node2 should now be submitted
        storageProvider.updateNodeStatus(operationId, node1, OperationalJobStatus.SUCCEEDED);
        executeAndWait(coordinator);

        // RUNNING is written synchronously during the poll before the promise executeAndWait blocks
        // on is completed, so no polling is needed.
        OperationalJobStatus node2SubmittedStatus = storageProvider.getNodeStatus(operationId, node2);
        assertThat(node2SubmittedStatus)
        .describedAs("Node2 should be RUNNING or SUCCEEDED after predecessor completed")
        .isIn(OperationalJobStatus.RUNNING, OperationalJobStatus.SUCCEEDED);

        // Scenario 3: Wait for node2 to finish, then verify job completion
        loopAssert(5, () -> {
            OperationalJobStatus status = storageProvider.getNodeStatus(operationId, node2);
            assertThat(status)
            .describedAs("Node2 should eventually reach SUCCEEDED")
            .isEqualTo(OperationalJobStatus.SUCCEEDED);
        });

        executeAndWait(coordinator);

        loopAssert(5, () -> {
            OperationalJobRecord finalRecord = storageProvider.findJob(operationId);
            assertThat(finalRecord).isNotNull();
            assertThat(finalRecord.status())
            .describedAs("Job should be finalized as SUCCEEDED when all nodes complete")
            .isEqualTo(OperationalJobStatus.SUCCEEDED);
        });

        assertThat(opCoordinator.getActiveOperation(OperationType.DRAIN))
        .describedAs("Active operation should be cleared after job completion")
        .isNull();
    }

    @CassandraIntegrationTest
    void abortedJobRecordIsFinalizedAndReleasesItsLock()
    {
        waitForSchemaReady(10, TimeUnit.SECONDS);

        StorageProvider storageProvider = injector.getInstance(StorageProvider.class);
        storageProvider.initialize();

        UUID operationId = UUIDs.timeBased();
        UUID node = UUID.randomUUID();

        // An abort whose nodes have all settled themselves. The handler never touches the lock, so releasing it
        // is left entirely to this poll.
        storageProvider.persistJob(OperationalJobRecord.builder()
                                                       .jobId(operationId)
                                                       .operationType(OperationType.DRAIN)
                                                       .status(OperationalJobStatus.ABORTED)
                                                       .failureReason(OPERATOR_ABORT_REASON)
                                                       .nodeExecutionOrder(Collections.singletonList(
                                                       Collections.singletonList(node)))
                                                       .build());
        assertThat(storageProvider.trySetActiveOperation(OperationType.DRAIN, operationId, null)).isTrue();
        storageProvider.updateNodeStatus(operationId, node, OperationalJobStatus.FAILED);

        OperationalJobCoordinator opCoordinator = new StorageBackedOperationalJobCoordinator(storageProvider);

        executeAndWait(coordinatorFor(storageProvider, opCoordinator, node));

        assertThat(opCoordinator.getActiveOperation(OperationType.DRAIN))
        .describedAs("the abort answers 202 on the promise that finalization releases the lock once every node "
                     + "row is terminal; this is that poll")
        .isNull();
    }

    @CassandraIntegrationTest
    void abortedJobRecordIsNotRewrittenByFinalization()
    {
        waitForSchemaReady(10, TimeUnit.SECONDS);

        StorageProvider storageProvider = injector.getInstance(StorageProvider.class);
        storageProvider.initialize();

        UUID operationId = UUIDs.timeBased();
        UUID node = UUID.randomUUID();

        storageProvider.persistJob(OperationalJobRecord.builder()
                                                       .jobId(operationId)
                                                       .operationType(OperationType.DRAIN)
                                                       .status(OperationalJobStatus.ABORTED)
                                                       .failureReason(OPERATOR_ABORT_REASON)
                                                       .nodeExecutionOrder(Collections.singletonList(
                                                       Collections.singletonList(node)))
                                                       .build());
        assertThat(storageProvider.trySetActiveOperation(OperationType.DRAIN, operationId, null)).isTrue();
        storageProvider.updateNodeStatus(operationId, node, OperationalJobStatus.FAILED);

        OperationalJobCoordinator opCoordinator = new StorageBackedOperationalJobCoordinator(storageProvider);

        executeAndWait(coordinatorFor(storageProvider, opCoordinator, node));

        OperationalJobRecord finalized = storageProvider.findJob(operationId);
        assertThat(finalized).isNotNull();
        assertThat(finalized.status())
        .describedAs("an operator killed this operation, and finalization must not restate that as a failure the "
                     + "operation had on its own")
        .isEqualTo(OperationalJobStatus.ABORTED);
        assertThat(finalized.failureReason())
        .describedAs("the generic finalization reason must not replace the one recorded when the operation was "
                     + "aborted by hand")
        .isEqualTo(OPERATOR_ABORT_REASON);
    }

    @CassandraIntegrationTest
    void abortedJobRecordSettlesThisSidecarsNodeAndReleasesTheLock()
    {
        waitForSchemaReady(10, TimeUnit.SECONDS);

        StorageProvider storageProvider = injector.getInstance(StorageProvider.class);
        storageProvider.initialize();

        UUID operationId = UUIDs.timeBased();
        UUID node = UUID.randomUUID();

        storageProvider.persistJob(OperationalJobRecord.builder()
                                                       .jobId(operationId)
                                                       .operationType(OperationType.DRAIN)
                                                       .status(OperationalJobStatus.ABORTED)
                                                       .failureReason(OPERATOR_ABORT_REASON)
                                                       .nodeExecutionOrder(Collections.singletonList(
                                                       Collections.singletonList(node)))
                                                       .build());
        assertThat(storageProvider.trySetActiveOperation(OperationType.DRAIN, operationId, null)).isTrue();
        storageProvider.updateNodeStatus(operationId, node, OperationalJobStatus.CREATED);

        OperationalJobCoordinator opCoordinator = new StorageBackedOperationalJobCoordinator(storageProvider);

        executeAndWait(coordinatorFor(storageProvider, opCoordinator, node));

        assertThat(storageProvider.getNodeStatus(operationId, node))
        .describedAs("a node that never started is settled by its own Sidecar, and records ABORTED because it was "
                     + "never touched")
        .isEqualTo(OperationalJobStatus.ABORTED);
        assertThat(opCoordinator.getActiveOperation(OperationType.DRAIN))
        .describedAs("with every row now terminal, finalization releases the lock; this poll completes the abort "
                     + "that the handler only started")
        .isNull();
    }

    @CassandraIntegrationTest
    void abortedJobRecordSettlesARunningNodeAsFailedAndRunsRecovery() throws InterruptedException
    {
        waitForSchemaReady(10, TimeUnit.SECONDS);

        StorageProvider storageProvider = injector.getInstance(StorageProvider.class);
        storageProvider.initialize();

        UUID operationId = UUIDs.timeBased();
        UUID node = UUID.randomUUID();

        storageProvider.persistJob(OperationalJobRecord.builder()
                                                       .jobId(operationId)
                                                       .operationType(OperationType.DRAIN)
                                                       .status(OperationalJobStatus.ABORTED)
                                                       .failureReason(OPERATOR_ABORT_REASON)
                                                       .nodeExecutionOrder(Collections.singletonList(
                                                       Collections.singletonList(node)))
                                                       .build());
        assertThat(storageProvider.trySetActiveOperation(OperationType.DRAIN, operationId, null)).isTrue();
        storageProvider.updateNodeStatus(operationId, node, OperationalJobStatus.RUNNING);

        CountDownLatch recoveryRan = new CountDownLatch(1);
        LocalJobFactory factory = (opId, nodeId, host, opType) -> new LocalJob(opId, nodeId, host, opType)
        {
            @Override
            protected void executeInternal()
            {
            }

            @Override
            public Future<Void> onJobFailed()
            {
                recoveryRan.countDown();
                return Future.succeededFuture();
            }
        };

        OperationalJobCoordinator opCoordinator = new StorageBackedOperationalJobCoordinator(storageProvider);

        executeAndWait(coordinatorFor(storageProvider, opCoordinator, node, factory));

        assertThat(storageProvider.getNodeStatus(operationId, node))
        .describedAs("a node abandoned mid-execution records FAILED rather than the operation's ABORTED, because "
                     + "the operation stopped without learning whether the work finished")
        .isEqualTo(OperationalJobStatus.FAILED);
        assertThat(recoveryRan.await(10, TimeUnit.SECONDS))
        .describedAs("settling the row is not enough for a node that was mid-execution; the job's own recovery has "
                     + "to run to reconcile whatever the abandoned work left behind")
        .isTrue();
        assertThat(opCoordinator.getActiveOperation(OperationType.DRAIN))
        .describedAs("with the last row now terminal, finalization releases the lock")
        .isNull();
    }

    private StorageBackedLocalJobCoordinator coordinatorFor(StorageProvider storageProvider,
                                                            OperationalJobCoordinator opCoordinator,
                                                            UUID localNode)
    {
        return coordinatorFor(storageProvider, opCoordinator, localNode,
                              (opId, nodeId, host, opType) -> new LocalJob(opId, nodeId, host, opType)
                              {
                                  @Override
                                  protected void executeInternal()
                                  {
                                  }
                              });
    }

    private StorageBackedLocalJobCoordinator coordinatorFor(StorageProvider storageProvider,
                                                            OperationalJobCoordinator opCoordinator,
                                                            UUID localNode,
                                                            LocalJobFactory factory)
    {
        ExecutorPools executorPools = injector.getInstance(ExecutorPools.class);
        return new StorageBackedLocalJobCoordinator(executorPools,
                                                    opCoordinator,
                                                    storageProvider,
                                                    mockFetcherForNode(localNode, "127.0.0.1"),
                                                    new LocalJobManager(executorPools),
                                                    injector.getInstance(SidecarConfiguration.class),
                                                    factory,
                                                    TimeProvider.DEFAULT_TIME_PROVIDER);
    }

    private InstanceMetadataFetcher mockFetcherForNode(UUID nodeId, String host)
    {
        InstanceMetadataFetcher fetcher = mock(InstanceMetadataFetcher.class);
        InstanceMetadata instance = mock(InstanceMetadata.class);
        CassandraAdapterDelegate delegate = mock(CassandraAdapterDelegate.class);
        NodeSettings nodeSettings = NodeSettings.builder().hostId(nodeId).build();

        when(instance.host()).thenReturn(host);
        when(instance.delegate()).thenReturn(delegate);
        when(delegate.nodeSettings()).thenReturn(nodeSettings);
        when(fetcher.allLocalInstances()).thenReturn(Collections.singletonList(instance));
        return fetcher;
    }

    private void executeAndWait(StorageBackedLocalJobCoordinator coordinator)
    {
        CountDownLatch latch = new CountDownLatch(1);
        Promise<Void> promise = Promise.promise();
        promise.future().onComplete(ar -> latch.countDown());
        coordinator.execute(promise);
        try
        {
            assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }
}
