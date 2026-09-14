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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import com.datastax.driver.core.utils.UUIDs;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.TestResourceReaper;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.data.OperationType;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.apache.cassandra.sidecar.common.response.NodeSettings;
import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.OperationalJobConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.config.yaml.OperationalJobConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.ServiceConfigurationImpl;
import org.apache.cassandra.sidecar.job.storage.OperationalJobRecord;
import org.apache.cassandra.sidecar.job.storage.StorageProvider;
import org.apache.cassandra.sidecar.tasks.ScheduleDecision;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.cassandra.sidecar.utils.TimeProvider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link StorageBackedLocalJobCoordinator}
 */
class StorageBackedLocalJobCoordinatorTest
{
    private static final UUID OPERATION_ID = UUIDs.timeBased();
    private static final UUID NODE_1 = UUID.randomUUID();
    private static final UUID NODE_2 = UUID.randomUUID();
    private static final UUID NODE_3 = UUID.randomUUID();

    private Vertx vertx;
    private ExecutorPools executorPools;
    private OperationalJobCoordinator operationalJobCoordinator;
    private StorageProvider storageProvider;
    private InstanceMetadataFetcher instanceMetadataFetcher;
    private LocalJobManager localJobManager;
    private StorageBackedLocalJobCoordinator coordinator;
    private LocalJobFactory localJobFactory;
    private final FakeTimeProvider timeProvider = new FakeTimeProvider();

    @BeforeEach
    void setup()
    {
        vertx = Vertx.vertx();
        executorPools = new ExecutorPools(vertx, new ServiceConfigurationImpl());
        operationalJobCoordinator = mock(OperationalJobCoordinator.class);
        storageProvider = mock(StorageProvider.class);
        instanceMetadataFetcher = mock(InstanceMetadataFetcher.class);
        localJobManager = new LocalJobManager(executorPools);
    }

    @AfterEach
    void cleanup()
    {
        TestResourceReaper.create().with(vertx).with(executorPools).close();
    }

    @Test
    void testIdleWhenNoActiveOperations()
    {
        buildCoordinator(defaultConfig());
        when(storageProvider.isAvailable()).thenReturn(true);
        when(operationalJobCoordinator.getActiveOperations()).thenReturn(Collections.emptyMap());

        executeAndWait();

        verify(storageProvider, never()).findJob(any());
    }

    @Test
    void testIdleWhenStorageProviderUnavailable()
    {
        buildCoordinator(defaultConfig());
        when(storageProvider.isAvailable()).thenReturn(false);

        executeAndWait();

        verify(operationalJobCoordinator, never()).getActiveOperations();
    }

    @Test
    void testDiscoverActiveOperationAndFetchJob()
    {
        buildCoordinator(defaultConfig());
        OperationalJobRecord jobRecord = createJobRecord(Arrays.asList(
        Arrays.asList(NODE_1),
        Arrays.asList(NODE_2)
        ));

        when(storageProvider.isAvailable()).thenReturn(true);
        when(operationalJobCoordinator.getActiveOperations())
        .thenReturn(Collections.singletonMap(OperationType.DRAIN, OPERATION_ID));
        when(storageProvider.findJob(OPERATION_ID)).thenReturn(jobRecord);
        when(instanceMetadataFetcher.allLocalInstances()).thenReturn(Collections.emptyList());

        executeAndWait();

        verify(storageProvider).findJob(OPERATION_ID);
    }

    @Test
    void testLocalNodeNotInExecutionOrder()
    {
        UUID otherNode = UUID.randomUUID();
        buildCoordinator(defaultConfig());
        OperationalJobRecord jobRecord = createJobRecord(Arrays.asList(
        Arrays.asList(otherNode)
        ));
        setupSingleNodeInstance(NODE_1, "127.0.0.1");

        when(storageProvider.isAvailable()).thenReturn(true);
        when(operationalJobCoordinator.getActiveOperations())
        .thenReturn(Collections.singletonMap(OperationType.DRAIN, OPERATION_ID));
        when(storageProvider.findJob(OPERATION_ID)).thenReturn(jobRecord);
        when(storageProvider.getNodeStatusesForOperation(OPERATION_ID)).thenReturn(Collections.emptyMap());

        executeAndWait();

        verify(storageProvider, never()).updateNodeStatus(any(), any(), any());
    }

    @Test
    void testWaitForPredecessorsWhenEarlierGroupsNotDone()
    {
        buildCoordinatorWithTestFactory(defaultConfig());
        OperationalJobRecord jobRecord = createJobRecord(Arrays.asList(
        Arrays.asList(NODE_1),
        Arrays.asList(NODE_2)
        ));
        setupSingleNodeInstance(NODE_2, "127.0.0.2");

        Map<UUID, OperationalJobStatus> statuses = new HashMap<>();
        statuses.put(NODE_1, OperationalJobStatus.RUNNING);

        when(storageProvider.isAvailable()).thenReturn(true);
        when(operationalJobCoordinator.getActiveOperations())
        .thenReturn(Collections.singletonMap(OperationType.DRAIN, OPERATION_ID));
        when(storageProvider.findJob(OPERATION_ID)).thenReturn(jobRecord);
        when(storageProvider.getNodeStatusesForOperation(OPERATION_ID)).thenReturn(statuses);

        executeAndWait();

        assertThat(localJobManager.getJob(OPERATION_ID, NODE_2)).isNull();
    }

    @Test
    void testPredecessorsCompleteAllSucceeded()
    {
        buildCoordinatorWithTestFactory(defaultConfig());
        OperationalJobRecord jobRecord = createJobRecord(Arrays.asList(
        Arrays.asList(NODE_1),
        Arrays.asList(NODE_2)
        ));
        setupSingleNodeInstance(NODE_2, "127.0.0.2");

        Map<UUID, OperationalJobStatus> statuses = new HashMap<>();
        statuses.put(NODE_1, OperationalJobStatus.SUCCEEDED);

        when(storageProvider.isAvailable()).thenReturn(true);
        when(operationalJobCoordinator.getActiveOperations())
        .thenReturn(Collections.singletonMap(OperationType.DRAIN, OPERATION_ID));
        when(storageProvider.findJob(OPERATION_ID)).thenReturn(jobRecord);
        when(storageProvider.getNodeStatusesForOperation(OPERATION_ID)).thenReturn(statuses);

        executeAndWait();

        verify(storageProvider).updateNodeStatus(OPERATION_ID, NODE_2, OperationalJobStatus.RUNNING);
    }

    @Test
    void testWaitBetweenExecutionGroupsBlocksSubmissionUntilElapsed()
    {
        localJobFactory = waitingTestFactory("60s");
        buildCoordinator(defaultConfig());

        OperationalJobRecord jobRecord = createJobRecord(Arrays.asList(
        Arrays.asList(NODE_1),
        Arrays.asList(NODE_2)
        ));
        setupSingleNodeInstance(NODE_2, "127.0.0.2");

        Map<UUID, OperationalJobStatus> statuses = new HashMap<>();
        statuses.put(NODE_1, OperationalJobStatus.SUCCEEDED);

        when(storageProvider.isAvailable()).thenReturn(true);
        when(operationalJobCoordinator.getActiveOperations())
        .thenReturn(Collections.singletonMap(OperationType.DRAIN, OPERATION_ID));
        when(storageProvider.findJob(OPERATION_ID)).thenReturn(jobRecord);
        when(storageProvider.getNodeStatusesForOperation(OPERATION_ID)).thenReturn(statuses);

        executeAndWait();
        verify(storageProvider, never()).updateNodeStatus(OPERATION_ID, NODE_2, OperationalJobStatus.RUNNING);

        timeProvider.advance(59, TimeUnit.SECONDS);
        executeAndWait();
        verify(storageProvider, never()).updateNodeStatus(OPERATION_ID, NODE_2, OperationalJobStatus.RUNNING);

        timeProvider.advance(2, TimeUnit.SECONDS);
        executeAndWait();
        verify(storageProvider).updateNodeStatus(OPERATION_ID, NODE_2, OperationalJobStatus.RUNNING);
    }

    @Test
    void testWaitBetweenExecutionGroupsRunsFromFirstObservationNotOperationStart()
    {
        localJobFactory = waitingTestFactory("60s");
        buildCoordinator(defaultConfig());

        OperationalJobRecord jobRecord = createJobRecord(Arrays.asList(
        Arrays.asList(NODE_1),
        Arrays.asList(NODE_2)
        ));
        setupSingleNodeInstance(NODE_2, "127.0.0.2");

        Map<UUID, OperationalJobStatus> statuses = new HashMap<>();
        statuses.put(NODE_1, OperationalJobStatus.RUNNING);

        when(storageProvider.isAvailable()).thenReturn(true);
        when(operationalJobCoordinator.getActiveOperations())
        .thenReturn(Collections.singletonMap(OperationType.DRAIN, OPERATION_ID));
        when(storageProvider.findJob(OPERATION_ID)).thenReturn(jobRecord);
        when(storageProvider.getNodeStatusesForOperation(OPERATION_ID)).thenReturn(statuses);

        // The predecessor spends well over the wait duration running, which must not count toward the settle window.
        executeAndWait();
        timeProvider.advance(10, TimeUnit.MINUTES);
        statuses.put(NODE_1, OperationalJobStatus.SUCCEEDED);

        executeAndWait();
        verify(storageProvider, never()).updateNodeStatus(OPERATION_ID, NODE_2, OperationalJobStatus.RUNNING);

        timeProvider.advance(61, TimeUnit.SECONDS);
        executeAndWait();
        verify(storageProvider).updateNodeStatus(OPERATION_ID, NODE_2, OperationalJobStatus.RUNNING);
    }

    @Test
    void testWaitBetweenExecutionGroupsDoesNotDelayTheFirstGroup()
    {
        localJobFactory = waitingTestFactory("60s");
        buildCoordinator(defaultConfig());

        OperationalJobRecord jobRecord = createJobRecord(Arrays.asList(
        Arrays.asList(NODE_1),
        Arrays.asList(NODE_2)
        ));
        setupSingleNodeInstance(NODE_1, "127.0.0.1");

        when(storageProvider.isAvailable()).thenReturn(true);
        when(operationalJobCoordinator.getActiveOperations())
        .thenReturn(Collections.singletonMap(OperationType.DRAIN, OPERATION_ID));
        when(storageProvider.findJob(OPERATION_ID)).thenReturn(jobRecord);
        when(storageProvider.getNodeStatusesForOperation(OPERATION_ID)).thenReturn(new HashMap<>());

        executeAndWait();

        verify(storageProvider).updateNodeStatus(OPERATION_ID, NODE_1, OperationalJobStatus.RUNNING);
    }

    @Test
    void testZeroWaitBetweenExecutionGroupsSubmitsImmediately()
    {
        localJobFactory = waitingTestFactory("0s");
        buildCoordinator(defaultConfig());

        OperationalJobRecord jobRecord = createJobRecord(Arrays.asList(
        Arrays.asList(NODE_1),
        Arrays.asList(NODE_2)
        ));
        setupSingleNodeInstance(NODE_2, "127.0.0.2");

        Map<UUID, OperationalJobStatus> statuses = new HashMap<>();
        statuses.put(NODE_1, OperationalJobStatus.SUCCEEDED);

        when(storageProvider.isAvailable()).thenReturn(true);
        when(operationalJobCoordinator.getActiveOperations())
        .thenReturn(Collections.singletonMap(OperationType.DRAIN, OPERATION_ID));
        when(storageProvider.findJob(OPERATION_ID)).thenReturn(jobRecord);
        when(storageProvider.getNodeStatusesForOperation(OPERATION_ID)).thenReturn(statuses);

        executeAndWait();

        verify(storageProvider).updateNodeStatus(OPERATION_ID, NODE_2, OperationalJobStatus.RUNNING);
    }

    @Test
    void testCrashRecoveryIsNotBlockedByTheInterGroupWait()
    {
        localJobFactory = new LocalJobFactory()
        {
            @Override
            public LocalJob createJob(UUID opId, UUID nodeId, String host, OperationType opType)
            {
                return new LocalJob(opId, nodeId, host, opType)
                {
                    @Override
                    protected void executeInternal()
                    {
                    }
                };
            }

            @Override
            public boolean canRecover()
            {
                return true;
            }

            @Override
            public SecondBoundConfiguration waitBetweenExecutionGroups(Map<String, String> operationMetadata)
            {
                return SecondBoundConfiguration.parse("60s");
            }
        };
        buildCoordinator(defaultConfig());

        OperationalJobRecord jobRecord = createJobRecord(Arrays.asList(
        Arrays.asList(NODE_1),
        Arrays.asList(NODE_2)
        ));
        setupSingleNodeInstance(NODE_2, "127.0.0.2");

        Map<UUID, OperationalJobStatus> statuses = new HashMap<>();
        statuses.put(NODE_1, OperationalJobStatus.SUCCEEDED);
        statuses.put(NODE_2, OperationalJobStatus.RUNNING);

        when(storageProvider.isAvailable()).thenReturn(true);
        when(operationalJobCoordinator.getActiveOperations())
        .thenReturn(Collections.singletonMap(OperationType.DRAIN, OPERATION_ID));
        when(storageProvider.findJob(OPERATION_ID)).thenReturn(jobRecord);
        when(storageProvider.getNodeStatusesForOperation(OPERATION_ID)).thenReturn(statuses);

        executeAndWait();

        // The node was already mid-execution before the crash, so its group has started and the settle window
        // for that boundary is already spent; holding the resubmit back would strand the node.
        verify(storageProvider).updateNodeStatus(OPERATION_ID, NODE_2, OperationalJobStatus.CREATED);
        verify(storageProvider).updateNodeStatus(OPERATION_ID, NODE_2, OperationalJobStatus.RUNNING);
    }

    @Test
    void testLocalNodeAlreadySucceeded()
    {
        buildCoordinatorWithTestFactory(defaultConfig());
        OperationalJobRecord jobRecord = createJobRecord(Arrays.asList(
        Arrays.asList(NODE_1)
        ));
        setupSingleNodeInstance(NODE_1, "127.0.0.1");

        Map<UUID, OperationalJobStatus> statuses = new HashMap<>();
        statuses.put(NODE_1, OperationalJobStatus.SUCCEEDED);

        when(storageProvider.isAvailable()).thenReturn(true);
        when(operationalJobCoordinator.getActiveOperations())
        .thenReturn(Collections.singletonMap(OperationType.DRAIN, OPERATION_ID));
        when(storageProvider.findJob(OPERATION_ID)).thenReturn(jobRecord);
        when(storageProvider.getNodeStatusesForOperation(OPERATION_ID)).thenReturn(statuses);

        executeAndWait();

        assertThat(localJobManager.getJob(OPERATION_ID, NODE_1)).isNull();
    }

    @Test
    void testLocalNodeAlreadyFailed()
    {
        buildCoordinatorWithTestFactory(defaultConfig());
        OperationalJobRecord jobRecord = createJobRecord(Arrays.asList(
        Arrays.asList(NODE_1)
        ));
        setupSingleNodeInstance(NODE_1, "127.0.0.1");

        Map<UUID, OperationalJobStatus> statuses = new HashMap<>();
        statuses.put(NODE_1, OperationalJobStatus.FAILED);

        when(storageProvider.isAvailable()).thenReturn(true);
        when(operationalJobCoordinator.getActiveOperations())
        .thenReturn(Collections.singletonMap(OperationType.DRAIN, OPERATION_ID));
        when(storageProvider.findJob(OPERATION_ID)).thenReturn(jobRecord);
        when(storageProvider.getNodeStatusesForOperation(OPERATION_ID)).thenReturn(statuses);

        executeAndWait();

        assertThat(localJobManager.getJob(OPERATION_ID, NODE_1)).isNull();
    }

    @Test
    void testMultipleLocalInstances()
    {
        buildCoordinatorWithTestFactory(defaultConfig());
        OperationalJobRecord jobRecord = createJobRecord(Arrays.asList(
        Arrays.asList(NODE_1, NODE_2)
        ));
        setupMultipleNodeInstances(
        new UUID[]{NODE_1, NODE_2},
        new String[]{"127.0.0.1", "127.0.0.2"}
        );

        when(storageProvider.isAvailable()).thenReturn(true);
        when(operationalJobCoordinator.getActiveOperations())
        .thenReturn(Collections.singletonMap(OperationType.DRAIN, OPERATION_ID));
        when(storageProvider.findJob(OPERATION_ID)).thenReturn(jobRecord);
        when(storageProvider.getNodeStatusesForOperation(OPERATION_ID)).thenReturn(new HashMap<>());

        executeAndWait();

        verify(storageProvider).updateNodeStatus(OPERATION_ID, NODE_1, OperationalJobStatus.RUNNING);
        verify(storageProvider).updateNodeStatus(OPERATION_ID, NODE_2, OperationalJobStatus.RUNNING);
    }

    @Test
    void testArePredecessorsCompleteBlocksWhenRequiresPredecessorSuccessTrue()
    {
        buildCoordinatorWithTestFactory(defaultConfig());

        List<List<UUID>> executionOrder = Arrays.asList(
        Arrays.asList(NODE_1),
        Arrays.asList(NODE_2)
        );

        Map<UUID, OperationalJobStatus> statuses = new HashMap<>();
        statuses.put(NODE_1, OperationalJobStatus.FAILED);

        assertThat(coordinator.arePredecessorsComplete(NODE_2, executionOrder, statuses, true)).isFalse();
    }

    @Test
    void testArePredecessorsCompleteAllowsProgressWhenRequiresPredecessorSuccessFalse()
    {
        buildCoordinatorWithTestFactory(defaultConfig());

        List<List<UUID>> executionOrder = Arrays.asList(
        Arrays.asList(NODE_1),
        Arrays.asList(NODE_2)
        );

        Map<UUID, OperationalJobStatus> statuses = new HashMap<>();
        statuses.put(NODE_1, OperationalJobStatus.FAILED);

        assertThat(coordinator.arePredecessorsComplete(NODE_2, executionOrder, statuses, false)).isTrue();
    }

    @Test
    void testArePredecessorsCompleteReturnsFalseForUnknownNode()
    {
        buildCoordinatorWithTestFactory(defaultConfig());

        UUID unknownNode = UUID.randomUUID();
        List<List<UUID>> executionOrder = Arrays.asList(
        Arrays.asList(NODE_1)
        );

        assertThat(coordinator.arePredecessorsComplete(unknownNode, executionOrder, new HashMap<>(), false)).isFalse();
    }

    @Test
    void testFailedPredecessorDoesNotBlockWhenRequiresPredecessorSuccessFalse()
    {
        buildCoordinatorWithTestFactory(defaultConfig());
        OperationalJobRecord jobRecord = createJobRecord(Arrays.asList(
        Arrays.asList(NODE_1),
        Arrays.asList(NODE_2)
        ));
        setupSingleNodeInstance(NODE_2, "127.0.0.2");

        Map<UUID, OperationalJobStatus> statuses = new HashMap<>();
        statuses.put(NODE_1, OperationalJobStatus.FAILED);

        when(storageProvider.isAvailable()).thenReturn(true);
        when(operationalJobCoordinator.getActiveOperations())
        .thenReturn(Collections.singletonMap(OperationType.DRAIN, OPERATION_ID));
        when(storageProvider.findJob(OPERATION_ID)).thenReturn(jobRecord);
        when(storageProvider.getNodeStatusesForOperation(OPERATION_ID)).thenReturn(statuses);

        executeAndWait();

        verify(storageProvider).updateNodeStatus(OPERATION_ID, NODE_2, OperationalJobStatus.RUNNING);
    }

    @Test
    void testFailedPredecessorReportsSuccessorFailedWhenRequiresPredecessorSuccessTrue()
    {
        localJobFactory = predecessorSuccessTestFactory();
        buildCoordinator(defaultConfig());
        OperationalJobRecord jobRecord = createJobRecord(Arrays.asList(
        Arrays.asList(NODE_1),
        Arrays.asList(NODE_2)
        ));
        setupSingleNodeInstance(NODE_2, "127.0.0.2");

        Map<UUID, OperationalJobStatus> statuses = new HashMap<>();
        statuses.put(NODE_1, OperationalJobStatus.FAILED);

        when(storageProvider.isAvailable()).thenReturn(true);
        when(operationalJobCoordinator.getActiveOperations())
        .thenReturn(Collections.singletonMap(OperationType.DRAIN, OPERATION_ID));
        when(storageProvider.findJob(OPERATION_ID)).thenReturn(jobRecord);
        when(storageProvider.getNodeStatusesForOperation(OPERATION_ID)).thenReturn(statuses);

        executeAndWait();

        // The successor is actively reported FAILED, not left as unsubmitted.
        verify(storageProvider).updateNodeStatus(OPERATION_ID, NODE_2, OperationalJobStatus.FAILED);
        verify(storageProvider, never()).updateNodeStatus(OPERATION_ID, NODE_2, OperationalJobStatus.RUNNING);
    }

    @Test
    void testNodeExecutionTimeoutMarksNodeFailed() throws InterruptedException
    {
        CountDownLatch hang = new CountDownLatch(1);
        localJobFactory = (opId, nodeId, host, opType) -> new LocalJob(opId, nodeId, host, opType)
        {
            @Override
            protected void executeInternal()
            {
                try
                {
                    hang.await();
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                }
            }
        };
        OperationalJobConfiguration config = OperationalJobConfigurationImpl.builder()
                                                                            .nodeExecutionTimeout(SecondBoundConfiguration.parse("1s"))
                                                                            .build();
        buildCoordinator(config);
        OperationalJobRecord jobRecord = createJobRecord(Arrays.asList(
        Arrays.asList(NODE_1)
        ));
        setupSingleNodeInstance(NODE_1, "127.0.0.1");

        when(storageProvider.isAvailable()).thenReturn(true);
        when(operationalJobCoordinator.getActiveOperations())
        .thenReturn(Collections.singletonMap(OperationType.DRAIN, OPERATION_ID));
        when(storageProvider.findJob(OPERATION_ID)).thenReturn(jobRecord);
        when(storageProvider.getNodeStatusesForOperation(OPERATION_ID)).thenReturn(new HashMap<>());

        try
        {
            executeAndWait();
            // The job hangs; the per-node execution timeout must terminalize it as FAILED
            verify(storageProvider, timeout(5000)).updateNodeStatus(OPERATION_ID, NODE_1, OperationalJobStatus.FAILED);
        }
        finally
        {
            hang.countDown();
        }
    }

    @Test
    void testJobSubmittedAndStatusReported()
    {
        buildCoordinatorWithTestFactory(defaultConfig());
        OperationalJobRecord jobRecord = createJobRecord(Arrays.asList(
        Arrays.asList(NODE_1)
        ));
        setupSingleNodeInstance(NODE_1, "127.0.0.1");

        when(storageProvider.isAvailable()).thenReturn(true);
        when(operationalJobCoordinator.getActiveOperations())
        .thenReturn(Collections.singletonMap(OperationType.DRAIN, OPERATION_ID));
        when(storageProvider.findJob(OPERATION_ID)).thenReturn(jobRecord);
        when(storageProvider.getNodeStatusesForOperation(OPERATION_ID)).thenReturn(new HashMap<>());

        executeAndWait();

        verify(storageProvider).updateNodeStatus(OPERATION_ID, NODE_1, OperationalJobStatus.RUNNING);
        verify(storageProvider, timeout(5000)).updateNodeStatus(OPERATION_ID, NODE_1,
                                                                OperationalJobStatus.SUCCEEDED);
    }

    @Test
    void testJobHandleIsHeldUntilTheTerminalRowIsWritten() throws InterruptedException
    {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        localJobFactory = (opId, nodeId, host, opType) -> new LocalJob(opId, nodeId, host, opType)
        {
            @Override
            protected void executeInternal()
            {
                started.countDown();
                try
                {
                    assertThat(release.await(5, TimeUnit.SECONDS)).isTrue();
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                }
            }
        };
        buildCoordinator(defaultConfig());
        setupSingleNodeInstance(NODE_1, "127.0.0.1");

        when(storageProvider.isAvailable()).thenReturn(true);
        when(operationalJobCoordinator.getActiveOperations())
        .thenReturn(Collections.singletonMap(OperationType.DRAIN, OPERATION_ID));
        when(storageProvider.findJob(OPERATION_ID))
        .thenReturn(createJobRecord(Arrays.asList(Arrays.asList(NODE_1))));
        when(storageProvider.getNodeStatusesForOperation(OPERATION_ID)).thenReturn(new HashMap<>());

        executeAndWait();
        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(localJobManager.getJob(OPERATION_ID, NODE_1))
        .describedAs("the handle is what tells settlement this Sidecar still owes the node a row")
        .isNotNull();

        release.countDown();
        verify(storageProvider, timeout(5000)).updateNodeStatus(OPERATION_ID, NODE_1,
                                                                OperationalJobStatus.SUCCEEDED);

        long deadline = System.currentTimeMillis() + 5000;
        while (localJobManager.getJob(OPERATION_ID, NODE_1) != null && System.currentTimeMillis() < deadline)
        {
            Thread.sleep(20);
        }
        assertThat(localJobManager.getJob(OPERATION_ID, NODE_1))
        .describedAs("once the row is written the node is settled and the handle must not keep blocking it")
        .isNull();
    }

    @Test
    void testNodeInFirstGroupAlwaysReady()
    {
        buildCoordinatorWithTestFactory(defaultConfig());
        OperationalJobRecord jobRecord = createJobRecord(Arrays.asList(
        Arrays.asList(NODE_1),
        Arrays.asList(NODE_2)
        ));
        setupSingleNodeInstance(NODE_1, "127.0.0.1");

        when(storageProvider.isAvailable()).thenReturn(true);
        when(operationalJobCoordinator.getActiveOperations())
        .thenReturn(Collections.singletonMap(OperationType.DRAIN, OPERATION_ID));
        when(storageProvider.findJob(OPERATION_ID)).thenReturn(jobRecord);
        when(storageProvider.getNodeStatusesForOperation(OPERATION_ID)).thenReturn(new HashMap<>());

        executeAndWait();

        verify(storageProvider).updateNodeStatus(OPERATION_ID, NODE_1, OperationalJobStatus.RUNNING);
    }

    @Test
    void testCompletionCallbackReportsSucceededToStorage()
    {
        buildCoordinatorWithTestFactory(defaultConfig());
        OperationalJobRecord jobRecord = createJobRecord(Arrays.asList(
        Arrays.asList(NODE_1)
        ));
        setupSingleNodeInstance(NODE_1, "127.0.0.1");

        when(storageProvider.isAvailable()).thenReturn(true);
        when(operationalJobCoordinator.getActiveOperations())
        .thenReturn(Collections.singletonMap(OperationType.DRAIN, OPERATION_ID));
        when(storageProvider.findJob(OPERATION_ID)).thenReturn(jobRecord);
        when(storageProvider.getNodeStatusesForOperation(OPERATION_ID)).thenReturn(new HashMap<>());

        executeAndWait();

        verify(storageProvider).updateNodeStatus(OPERATION_ID, NODE_1, OperationalJobStatus.RUNNING);
        verify(storageProvider, timeout(2000)).updateNodeStatus(OPERATION_ID, NODE_1, OperationalJobStatus.SUCCEEDED);
    }

    @Test
    void testCompletionCallbackReportsFailedToStorage()
    {
        localJobFactory = (opId, nodeId, host, opType) -> new LocalJob(opId, nodeId, host, opType)
        {
            @Override
            protected void executeInternal()
            {
                throw new RuntimeException("job error");
            }
        };
        buildCoordinator(defaultConfig());

        OperationalJobRecord jobRecord = createJobRecord(Arrays.asList(
        Arrays.asList(NODE_1)
        ));
        setupSingleNodeInstance(NODE_1, "127.0.0.1");

        when(storageProvider.isAvailable()).thenReturn(true);
        when(operationalJobCoordinator.getActiveOperations())
        .thenReturn(Collections.singletonMap(OperationType.DRAIN, OPERATION_ID));
        when(storageProvider.findJob(OPERATION_ID)).thenReturn(jobRecord);
        when(storageProvider.getNodeStatusesForOperation(OPERATION_ID)).thenReturn(new HashMap<>());

        executeAndWait();

        verify(storageProvider).updateNodeStatus(OPERATION_ID, NODE_1, OperationalJobStatus.RUNNING);
        verify(storageProvider, timeout(2000)).updateNodeStatus(OPERATION_ID, NODE_1, OperationalJobStatus.FAILED);
    }

    @Test
    void testClearActiveOperationWhenAllNodesSucceeded()
    {
        buildCoordinatorWithTestFactory(defaultConfig());
        OperationalJobRecord jobRecord = createJobRecord(Arrays.asList(
        Arrays.asList(NODE_1)
        ));
        setupSingleNodeInstance(NODE_1, "127.0.0.1");

        Map<UUID, OperationalJobStatus> statuses = new HashMap<>();
        statuses.put(NODE_1, OperationalJobStatus.SUCCEEDED);

        when(storageProvider.isAvailable()).thenReturn(true);
        when(operationalJobCoordinator.getActiveOperations())
        .thenReturn(Collections.singletonMap(OperationType.DRAIN, OPERATION_ID));
        when(storageProvider.findJob(OPERATION_ID)).thenReturn(jobRecord);
        when(storageProvider.getNodeStatusesForOperation(OPERATION_ID)).thenReturn(statuses);
        when(operationalJobCoordinator.clearActive(OperationType.DRAIN, OPERATION_ID, null)).thenReturn(true);

        executeAndWait();

        verify(storageProvider).updateJobStatus(eq(OPERATION_ID), eq(OperationType.DRAIN),
                                                eq(OperationalJobStatus.SUCCEEDED), any());
        verify(operationalJobCoordinator).clearActive(OperationType.DRAIN, OPERATION_ID, null);
    }

    @Test
    void testClearActiveOperationWhenSomeNodesFailed()
    {
        buildCoordinatorWithTestFactory(defaultConfig());

        OperationalJobRecord jobRecord = createJobRecord(Arrays.asList(
        Arrays.asList(NODE_1, NODE_2)
        ));
        setupSingleNodeInstance(NODE_1, "127.0.0.1");

        Map<UUID, OperationalJobStatus> statuses = new HashMap<>();
        statuses.put(NODE_1, OperationalJobStatus.SUCCEEDED);
        statuses.put(NODE_2, OperationalJobStatus.FAILED);

        when(storageProvider.isAvailable()).thenReturn(true);
        when(operationalJobCoordinator.getActiveOperations())
        .thenReturn(Collections.singletonMap(OperationType.DRAIN, OPERATION_ID));
        when(storageProvider.findJob(OPERATION_ID)).thenReturn(jobRecord);
        when(storageProvider.getNodeStatusesForOperation(OPERATION_ID)).thenReturn(statuses);
        when(operationalJobCoordinator.clearActive(OperationType.DRAIN, OPERATION_ID, null)).thenReturn(true);

        executeAndWait();

        verify(storageProvider).updateJobStatus(eq(OPERATION_ID), eq(OperationType.DRAIN),
                                                eq(OperationalJobStatus.FAILED), any());
        verify(operationalJobCoordinator).clearActive(OperationType.DRAIN, OPERATION_ID, null);
    }

    @Test
    void testRecoveryRunsWhenLocalJobFails() throws InterruptedException
    {
        CountDownLatch recovered = new CountDownLatch(1);
        localJobFactory = (opId, nodeId, host, opType) -> new LocalJob(opId, nodeId, host, opType)
        {
            @Override
            protected void executeInternal()
            {
                throw new RuntimeException("Exception");
            }

            @Override
            public Future<Void> onJobFailed()
            {
                recovered.countDown();
                return Future.succeededFuture();
            }
        };
        buildCoordinator(defaultConfig());

        OperationalJobRecord jobRecord = createJobRecord(Arrays.asList(Arrays.asList(NODE_1)));
        setupSingleNodeInstance(NODE_1, "127.0.0.1");

        when(storageProvider.isAvailable()).thenReturn(true);
        when(operationalJobCoordinator.getActiveOperations())
        .thenReturn(Collections.singletonMap(OperationType.DRAIN, OPERATION_ID));
        when(storageProvider.findJob(OPERATION_ID)).thenReturn(jobRecord);
        when(storageProvider.getNodeStatusesForOperation(OPERATION_ID)).thenReturn(new HashMap<>());

        executeAndWait();

        verify(storageProvider, timeout(5000)).updateNodeStatus(OPERATION_ID, NODE_1, OperationalJobStatus.FAILED);
        assertThat(recovered.await(5, TimeUnit.SECONDS))
            .describedAs("Recovery must run on the node's own FAILED transition")
            .isTrue();
    }

    @Test
    void testTimeoutCancelsLocalJobAndRunsRecovery() throws InterruptedException
    {
        CountDownLatch recovered = new CountDownLatch(1);
        AtomicBoolean cancelObserved = new AtomicBoolean(false);
        localJobFactory = (opId, nodeId, host, opType) -> new LocalJob(opId, nodeId, host, opType)
        {
            @Override
            protected void executeInternal()
            {
                // Run until the coordinator cancels on timeout, then fail.
                while (!isCancelled())
                {
                    try
                    {
                        Thread.sleep(20);
                    }
                    catch (InterruptedException e)
                    {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
                cancelObserved.set(true);
                throw new RuntimeException("cancelled");
            }

            @Override
            public Future<Void> onJobFailed()
            {
                recovered.countDown();
                return Future.succeededFuture();
            }
        };
        OperationalJobConfiguration config = OperationalJobConfigurationImpl.builder()
                                                                            .nodeExecutionTimeout(SecondBoundConfiguration.parse("1s"))
                                                                            .build();
        buildCoordinator(config);

        OperationalJobRecord jobRecord = createJobRecord(Arrays.asList(Arrays.asList(NODE_1)));
        setupSingleNodeInstance(NODE_1, "127.0.0.1");

        when(storageProvider.isAvailable()).thenReturn(true);
        when(operationalJobCoordinator.getActiveOperations())
        .thenReturn(Collections.singletonMap(OperationType.DRAIN, OPERATION_ID));
        when(storageProvider.findJob(OPERATION_ID)).thenReturn(jobRecord);
        when(storageProvider.getNodeStatusesForOperation(OPERATION_ID)).thenReturn(new HashMap<>());

        executeAndWait();

        verify(storageProvider, timeout(5000)).updateNodeStatus(OPERATION_ID, NODE_1, OperationalJobStatus.FAILED);
        // Await recovery first: the job sets cancelObserved and throws before its completion runs recovery, so
        // once the recovery latch trips the cancellation has definitely been observed 
        assertThat(recovered.await(5, TimeUnit.SECONDS))
            .describedAs("Recovery must run after the timed-out job unwinds")
            .isTrue();
        assertThat(cancelObserved.get())
            .describedAs("The running job must observe cancellation on timeout")
            .isTrue();
    }

    @Test
    void testRecoveryNotRunWhenLocalJobSucceeds()
    {
        AtomicBoolean recovered = new AtomicBoolean(false);
        localJobFactory = (opId, nodeId, host, opType) -> new LocalJob(opId, nodeId, host, opType)
        {
            @Override
            protected void executeInternal()
            {
            }

            @Override
            public Future<Void> onJobFailed()
            {
                recovered.set(true);
                return Future.succeededFuture();
            }
        };
        buildCoordinator(defaultConfig());

        OperationalJobRecord jobRecord = createJobRecord(Arrays.asList(Arrays.asList(NODE_1)));
        setupSingleNodeInstance(NODE_1, "127.0.0.1");

        when(storageProvider.isAvailable()).thenReturn(true);
        when(operationalJobCoordinator.getActiveOperations())
        .thenReturn(Collections.singletonMap(OperationType.DRAIN, OPERATION_ID));
        when(storageProvider.findJob(OPERATION_ID)).thenReturn(jobRecord);
        when(storageProvider.getNodeStatusesForOperation(OPERATION_ID)).thenReturn(new HashMap<>());

        executeAndWait();

        // SUCCEEDED and the recovery decision are made in the same completion callback, so once the
        // success write is observed the callback has already passed the (skipped) recovery branch.
        verify(storageProvider, timeout(5000)).updateNodeStatus(OPERATION_ID, NODE_1, OperationalJobStatus.SUCCEEDED);
        assertThat(recovered.get())
            .describedAs("Recovery must not run for a successful job")
            .isFalse();
    }

    @Test
    void testRecoveryFailureDoesNotPropagate() throws InterruptedException
    {
        CountDownLatch recoveryAttempted = new CountDownLatch(1);
        localJobFactory = (opId, nodeId, host, opType) -> new LocalJob(opId, nodeId, host, opType)
        {
            @Override
            protected void executeInternal()
            {
                throw new RuntimeException("Exception");
            }

            @Override
            public Future<Void> onJobFailed()
            {
                recoveryAttempted.countDown();
                return Future.failedFuture(new RuntimeException("Reconciliation error"));
            }
        };
        buildCoordinator(defaultConfig());

        OperationalJobRecord jobRecord = createJobRecord(Arrays.asList(Arrays.asList(NODE_1)));
        setupSingleNodeInstance(NODE_1, "127.0.0.1");

        when(storageProvider.isAvailable()).thenReturn(true);
        when(operationalJobCoordinator.getActiveOperations())
        .thenReturn(Collections.singletonMap(OperationType.DRAIN, OPERATION_ID));
        when(storageProvider.findJob(OPERATION_ID)).thenReturn(jobRecord);
        when(storageProvider.getNodeStatusesForOperation(OPERATION_ID)).thenReturn(new HashMap<>());

        executeAndWait();

        verify(storageProvider, timeout(5000)).updateNodeStatus(OPERATION_ID, NODE_1, OperationalJobStatus.FAILED);
        assertThat(recoveryAttempted.await(5, TimeUnit.SECONDS))
            .describedAs("A failing recovery future is swallowed, not propagated")
            .isTrue();
    }

    @Test
    void testDoNotClearWhenNodesStillRunning()
    {
        buildCoordinatorWithTestFactory(defaultConfig());
        OperationalJobRecord jobRecord = createJobRecord(Arrays.asList(
        Arrays.asList(NODE_1, NODE_2)
        ));
        setupSingleNodeInstance(NODE_1, "127.0.0.1");

        Map<UUID, OperationalJobStatus> statuses = new HashMap<>();
        statuses.put(NODE_1, OperationalJobStatus.SUCCEEDED);
        statuses.put(NODE_2, OperationalJobStatus.RUNNING);

        when(storageProvider.isAvailable()).thenReturn(true);
        when(operationalJobCoordinator.getActiveOperations())
        .thenReturn(Collections.singletonMap(OperationType.DRAIN, OPERATION_ID));
        when(storageProvider.findJob(OPERATION_ID)).thenReturn(jobRecord);
        when(storageProvider.getNodeStatusesForOperation(OPERATION_ID)).thenReturn(statuses);

        executeAndWait();

        verify(operationalJobCoordinator, never()).clearActive(any(), any(), any());
    }

    @Test
    void testClearActiveRaceCondition()
    {
        buildCoordinatorWithTestFactory(defaultConfig());
        OperationalJobRecord jobRecord = createJobRecord(Arrays.asList(
        Arrays.asList(NODE_1)
        ));
        setupSingleNodeInstance(NODE_1, "127.0.0.1");

        Map<UUID, OperationalJobStatus> statuses = new HashMap<>();
        statuses.put(NODE_1, OperationalJobStatus.SUCCEEDED);

        when(storageProvider.isAvailable()).thenReturn(true);
        when(operationalJobCoordinator.getActiveOperations())
        .thenReturn(Collections.singletonMap(OperationType.DRAIN, OPERATION_ID));
        when(storageProvider.findJob(OPERATION_ID)).thenReturn(jobRecord);
        when(storageProvider.getNodeStatusesForOperation(OPERATION_ID)).thenReturn(statuses);
        when(operationalJobCoordinator.clearActive(OperationType.DRAIN, OPERATION_ID, null)).thenReturn(false);

        executeAndWait();

        verify(operationalJobCoordinator).clearActive(OperationType.DRAIN, OPERATION_ID, null);
    }

    @Test
    void testCrashRecoveryCanRecover()
    {
        localJobFactory = new LocalJobFactory()
        {
            @Override
            public LocalJob createJob(UUID opId, UUID nodeId, String host, OperationType opType)
            {
                return new LocalJob(opId, nodeId, host, opType)
                {
                    @Override
                    protected void executeInternal()
                    {
                    }
                };
            }

            @Override
            public boolean canRecover()
            {
                return true;
            }
        };
        buildCoordinator(defaultConfig());

        OperationalJobRecord jobRecord = createJobRecord(Arrays.asList(
        Arrays.asList(NODE_1)
        ));
        setupSingleNodeInstance(NODE_1, "127.0.0.1");

        Map<UUID, OperationalJobStatus> statuses = new HashMap<>();
        statuses.put(NODE_1, OperationalJobStatus.RUNNING);

        when(storageProvider.isAvailable()).thenReturn(true);
        when(operationalJobCoordinator.getActiveOperations())
        .thenReturn(Collections.singletonMap(OperationType.DRAIN, OPERATION_ID));
        when(storageProvider.findJob(OPERATION_ID)).thenReturn(jobRecord);
        when(storageProvider.getNodeStatusesForOperation(OPERATION_ID)).thenReturn(statuses);

        executeAndWait();

        verify(storageProvider).updateNodeStatus(OPERATION_ID, NODE_1, OperationalJobStatus.CREATED);
        verify(storageProvider).updateNodeStatus(OPERATION_ID, NODE_1, OperationalJobStatus.RUNNING);
    }

    @Test
    void testCrashRecoveryCannotRecover()
    {
        buildCoordinatorWithTestFactory(defaultConfig());
        OperationalJobRecord jobRecord = createJobRecord(Arrays.asList(
        Arrays.asList(NODE_1)
        ));
        setupSingleNodeInstance(NODE_1, "127.0.0.1");

        Map<UUID, OperationalJobStatus> statuses = new HashMap<>();
        statuses.put(NODE_1, OperationalJobStatus.RUNNING);

        when(storageProvider.isAvailable()).thenReturn(true);
        when(operationalJobCoordinator.getActiveOperations())
        .thenReturn(Collections.singletonMap(OperationType.DRAIN, OPERATION_ID));
        when(storageProvider.findJob(OPERATION_ID)).thenReturn(jobRecord);
        when(storageProvider.getNodeStatusesForOperation(OPERATION_ID)).thenReturn(statuses);

        executeAndWait();

        verify(storageProvider).updateNodeStatus(OPERATION_ID, NODE_1, OperationalJobStatus.FAILED);
    }

    @Test
    void testCrashRecoveryCannotRecoverStillRunsOnJobFailed()
    {
        AtomicBoolean onJobFailedInvoked = new AtomicBoolean(false);
        // default canRecover is false
        localJobFactory = (opId, nodeId, host, opType) -> new LocalJob(opId, nodeId, host, opType)
        {
            @Override
            protected void executeInternal()
            {
            }

            @Override
            public Future<Void> onJobFailed()
            {
                onJobFailedInvoked.set(true);
                return Future.succeededFuture();
            }
        };
        buildCoordinator(defaultConfig());

        OperationalJobRecord jobRecord = createJobRecord(Arrays.asList(
        Arrays.asList(NODE_1)
        ));
        setupSingleNodeInstance(NODE_1, "127.0.0.1");

        Map<UUID, OperationalJobStatus> statuses = new HashMap<>();
        statuses.put(NODE_1, OperationalJobStatus.RUNNING);

        when(storageProvider.isAvailable()).thenReturn(true);
        when(operationalJobCoordinator.getActiveOperations())
        .thenReturn(Collections.singletonMap(OperationType.DRAIN, OPERATION_ID));
        when(storageProvider.findJob(OPERATION_ID)).thenReturn(jobRecord);
        when(storageProvider.getNodeStatusesForOperation(OPERATION_ID)).thenReturn(statuses);

        executeAndWait();

        verify(storageProvider).updateNodeStatus(OPERATION_ID, NODE_1, OperationalJobStatus.FAILED);
        assertThat(onJobFailedInvoked).isTrue();
    }

    @Test
    void testCrashRecoveryNodePending()
    {
        buildCoordinatorWithTestFactory(defaultConfig());
        OperationalJobRecord jobRecord = createJobRecord(Arrays.asList(
        Arrays.asList(NODE_1)
        ));
        setupSingleNodeInstance(NODE_1, "127.0.0.1");

        Map<UUID, OperationalJobStatus> statuses = new HashMap<>();
        statuses.put(NODE_1, OperationalJobStatus.CREATED);

        when(storageProvider.isAvailable()).thenReturn(true);
        when(operationalJobCoordinator.getActiveOperations())
        .thenReturn(Collections.singletonMap(OperationType.DRAIN, OPERATION_ID));
        when(storageProvider.findJob(OPERATION_ID)).thenReturn(jobRecord);
        when(storageProvider.getNodeStatusesForOperation(OPERATION_ID)).thenReturn(statuses);

        executeAndWait();

        verify(storageProvider).updateNodeStatus(OPERATION_ID, NODE_1, OperationalJobStatus.RUNNING);
    }

    @Test
    void testMultipleConcurrentActiveOperations()
    {
        buildCoordinatorWithTestFactory(defaultConfig());

        UUID drainOperationId = OPERATION_ID;
        UUID repairOperationId = UUIDs.timeBased();

        OperationalJobRecord drainJob = createJobRecord(OperationType.DRAIN, Arrays.asList(
        Arrays.asList(NODE_1)
        ));
        OperationalJobRecord repairJob = OperationalJobRecord.builder()
                                                             .jobId(repairOperationId)
                                                             .operationType(OperationType.REPAIR)
                                                             .status(OperationalJobStatus.RUNNING)
                                                             .nodeExecutionOrder(Arrays.asList(Arrays.asList(NODE_2)))
                                                             .build();

        setupMultipleNodeInstances(
        new UUID[]{NODE_1, NODE_2},
        new String[]{"127.0.0.1", "127.0.0.2"}
        );

        Map<OperationType, UUID> activeOps = new HashMap<>();
        activeOps.put(OperationType.DRAIN, drainOperationId);
        activeOps.put(OperationType.REPAIR, repairOperationId);

        when(storageProvider.isAvailable()).thenReturn(true);
        when(operationalJobCoordinator.getActiveOperations()).thenReturn(activeOps);
        when(storageProvider.findJob(drainOperationId)).thenReturn(drainJob);
        when(storageProvider.findJob(repairOperationId)).thenReturn(repairJob);
        when(storageProvider.getNodeStatusesForOperation(drainOperationId)).thenReturn(new HashMap<>());
        when(storageProvider.getNodeStatusesForOperation(repairOperationId)).thenReturn(new HashMap<>());

        executeAndWait();

        verify(storageProvider).updateNodeStatus(drainOperationId, NODE_1, OperationalJobStatus.RUNNING);
        verify(storageProvider).updateNodeStatus(repairOperationId, NODE_2, OperationalJobStatus.RUNNING);
    }

    @Test
    void testStorageProviderExceptionHandledGracefully()
    {
        buildCoordinatorWithTestFactory(defaultConfig());
        when(storageProvider.isAvailable()).thenReturn(true);
        when(operationalJobCoordinator.getActiveOperations())
        .thenThrow(new RuntimeException("storage error"));

        executeAndWait();
    }

    @Test
    void testScheduleDecisionSkipsWhenDisabled()
    {
        OperationalJobConfiguration config = OperationalJobConfigurationImpl.builder()
                                                                            .coordinationEnabled(false)
                                                                            .build();
        buildCoordinator(config);
        assertThat(coordinator.scheduleDecision()).isEqualTo(ScheduleDecision.SKIP);
    }

    @Test
    void testRandomizedInitialDelay()
    {
        buildCoordinator(defaultConfig());
        long delayMs = coordinator.delay().toMillis();
        long initialDelayMs = coordinator.initialDelay().toMillis();
        assertThat(initialDelayMs).isBetween(0L, delayMs);
    }

    @Test
    void testFailedJobRecordStopsFurtherSubmission()
    {
        buildCoordinatorWithTestFactory(defaultConfig());
        OperationalJobRecord jobRecord = createJobRecord(OperationType.DRAIN, OperationalJobStatus.FAILED,
                                                         Arrays.asList(Arrays.asList(NODE_1)));
        setupSingleNodeInstance(NODE_1, "127.0.0.1");

        when(storageProvider.isAvailable()).thenReturn(true);
        when(operationalJobCoordinator.getActiveOperations())
        .thenReturn(Collections.singletonMap(OperationType.DRAIN, OPERATION_ID));
        when(storageProvider.findJob(OPERATION_ID)).thenReturn(jobRecord);
        when(storageProvider.getNodeStatusesForOperation(OPERATION_ID)).thenReturn(new HashMap<>());

        executeAndWait();

        verify(storageProvider, never()).updateNodeStatus(OPERATION_ID, NODE_1, OperationalJobStatus.RUNNING);
        assertThat(localJobManager.getJob(OPERATION_ID, NODE_1)).isNull();
    }

    @ParameterizedTest
    @EnumSource(value = OperationalJobStatus.class, names = {"FAILED", "ABORTED"})
    void testTerminalJobRecordCancelsInFlightLocalJob(OperationalJobStatus terminalStatus)
    throws InterruptedException
    {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch cancelObserved = new CountDownLatch(1);
        localJobFactory = (opId, nodeId, host, opType) -> new LocalJob(opId, nodeId, host, opType)
        {
            @Override
            protected void executeInternal()
            {
                started.countDown();
                while (!isCancelled())
                {
                    try
                    {
                        Thread.sleep(20);
                    }
                    catch (InterruptedException e)
                    {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
                cancelObserved.countDown();
            }
        };
        buildCoordinator(defaultConfig());

        List<List<UUID>> executionOrder = Arrays.asList(Arrays.asList(NODE_1));
        OperationalJobRecord running = createJobRecord(OperationType.DRAIN, OperationalJobStatus.RUNNING,
                                                       executionOrder);
        OperationalJobRecord terminal = createJobRecord(OperationType.DRAIN, terminalStatus, executionOrder);
        setupSingleNodeInstance(NODE_1, "127.0.0.1");

        Map<UUID, OperationalJobStatus> statuses = new HashMap<>();

        when(storageProvider.isAvailable()).thenReturn(true);
        when(operationalJobCoordinator.getActiveOperations())
        .thenReturn(Collections.singletonMap(OperationType.DRAIN, OPERATION_ID));
        when(storageProvider.findJob(OPERATION_ID)).thenReturn(running, terminal);
        when(storageProvider.getNodeStatusesForOperation(OPERATION_ID)).thenReturn(statuses);

        executeAndWait();
        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();

        statuses.put(NODE_1, OperationalJobStatus.RUNNING);
        executeAndWait();

        assertThat(cancelObserved.await(5, TimeUnit.SECONDS))
        .describedAs("A job still running for an operation recorded as " + terminalStatus
                     + " must be cancelled, not just dropped")
        .isTrue();
    }

    @Test
    void testLocalJobCancelledOnceItsOperationNoLongerHoldsALock() throws InterruptedException
    {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch cancelObserved = new CountDownLatch(1);
        localJobFactory = (opId, nodeId, host, opType) -> new LocalJob(opId, nodeId, host, opType)
        {
            @Override
            protected void executeInternal()
            {
                started.countDown();
                while (!isCancelled())
                {
                    try
                    {
                        Thread.sleep(20);
                    }
                    catch (InterruptedException e)
                    {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
                cancelObserved.countDown();
            }
        };
        buildCoordinator(defaultConfig());

        List<List<UUID>> executionOrder = Arrays.asList(Arrays.asList(NODE_1));
        OperationalJobRecord running = createJobRecord(OperationType.DRAIN, OperationalJobStatus.RUNNING,
                                                       executionOrder);
        setupSingleNodeInstance(NODE_1, "127.0.0.1");

        when(storageProvider.isAvailable()).thenReturn(true);
        when(storageProvider.findJob(OPERATION_ID)).thenReturn(running);
        when(storageProvider.getNodeStatusesForOperation(OPERATION_ID)).thenReturn(new HashMap<>());
        when(operationalJobCoordinator.getActiveOperations())
        .thenReturn(Collections.singletonMap(OperationType.DRAIN, OPERATION_ID));

        executeAndWait();
        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();

        // The operation's lock is gone, which can happen if a force abort settles rows this Sidecar 
        // does not own
        UUID otherOperationId = UUIDs.timeBased();
        when(operationalJobCoordinator.getActiveOperations())
        .thenReturn(Collections.singletonMap(OperationType.RESTART, otherOperationId));
        when(storageProvider.findJob(otherOperationId)).thenReturn(null);

        executeAndWait();

        assertThat(cancelObserved.await(5, TimeUnit.SECONDS))
        .describedAs("a job whose operation no longer holds a lock must be cancelled, not left to its own timeout")
        .isTrue();
    }
    
    @Test
    void testCancelledJobThatRanToCompletionStillReportsSucceeded() throws InterruptedException
    {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        localJobFactory = (opId, nodeId, host, opType) -> new LocalJob(opId, nodeId, host, opType)
        {
            @Override
            protected void executeInternal()
            {
                started.countDown();
                try
                {
                    // Returns normally without re-checking isCancelled(), standing in for a job that slips past
                    // its last checkpoint before the cancellation lands.
                    assertThat(release.await(5, TimeUnit.SECONDS)).isTrue();
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                }
            }
        };
        buildCoordinator(defaultConfig());

        List<List<UUID>> executionOrder = Arrays.asList(Arrays.asList(NODE_1));
        setupSingleNodeInstance(NODE_1, "127.0.0.1");

        when(storageProvider.isAvailable()).thenReturn(true);
        when(storageProvider.findJob(OPERATION_ID))
        .thenReturn(createJobRecord(OperationType.DRAIN, OperationalJobStatus.RUNNING, executionOrder));
        when(storageProvider.getNodeStatusesForOperation(OPERATION_ID)).thenReturn(new HashMap<>());
        when(operationalJobCoordinator.getActiveOperations())
        .thenReturn(Collections.singletonMap(OperationType.DRAIN, OPERATION_ID));

        executeAndWait();
        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();

        UUID otherOperationId = UUIDs.timeBased();
        when(operationalJobCoordinator.getActiveOperations())
        .thenReturn(Collections.singletonMap(OperationType.RESTART, otherOperationId));
        when(storageProvider.findJob(otherOperationId)).thenReturn(null);
        executeAndWait();

        release.countDown();

        // The node really did complete its work, so SUCCEEDED is the truthful record even though the operation
        // was abandoned and the job cancelled.
        verify(storageProvider, timeout(5000))
        .updateNodeStatus(OPERATION_ID, NODE_1, OperationalJobStatus.SUCCEEDED);
        verify(storageProvider, never()).updateNodeStatus(OPERATION_ID, NODE_1, OperationalJobStatus.FAILED);
    }

    @ParameterizedTest
    @EnumSource(value = OperationalJobStatus.class, names = {"FAILED", "ABORTED"})
    void testFinalizationReleasesLocksForAnAlreadyTerminalJobRecord(OperationalJobStatus terminalStatus)
    {
        buildCoordinatorWithTestFactory(defaultConfig());
        OperationalJobRecord jobRecord = createJobRecord(OperationType.DRAIN, terminalStatus,
                                                         Arrays.asList(Arrays.asList(NODE_1)));
        setupSingleNodeInstance(NODE_1, "127.0.0.1");

        Map<UUID, OperationalJobStatus> statuses = new HashMap<>();
        statuses.put(NODE_1, terminalStatus);

        when(storageProvider.isAvailable()).thenReturn(true);
        when(operationalJobCoordinator.getActiveOperations())
        .thenReturn(Collections.singletonMap(OperationType.DRAIN, OPERATION_ID));
        when(storageProvider.findJob(OPERATION_ID)).thenReturn(jobRecord);
        when(storageProvider.getNodeStatusesForOperation(OPERATION_ID)).thenReturn(statuses);
        when(operationalJobCoordinator.clearActive(OperationType.DRAIN, OPERATION_ID, null)).thenReturn(true);

        executeAndWait();

        // Self-heal: a finalization whose clearActive did not take leaves a terminal record still holding its lock,
        // and only a later poll re-running finalization can release it.
        verify(operationalJobCoordinator).clearActive(OperationType.DRAIN, OPERATION_ID, null);
    }

    @ParameterizedTest
    @EnumSource(value = OperationalJobStatus.class, names = {"FAILED", "ABORTED"})
    void testFinalizationDoesNotRewriteAnAlreadyTerminalJobStatus(OperationalJobStatus terminalStatus)
    {
        buildCoordinatorWithTestFactory(defaultConfig());
        OperationalJobRecord jobRecord = createJobRecord(OperationType.DRAIN, terminalStatus,
                                                         Arrays.asList(Arrays.asList(NODE_1)));
        setupSingleNodeInstance(NODE_1, "127.0.0.1");

        Map<UUID, OperationalJobStatus> statuses = new HashMap<>();
        statuses.put(NODE_1, terminalStatus);

        when(storageProvider.isAvailable()).thenReturn(true);
        when(operationalJobCoordinator.getActiveOperations())
        .thenReturn(Collections.singletonMap(OperationType.DRAIN, OPERATION_ID));
        when(storageProvider.findJob(OPERATION_ID)).thenReturn(jobRecord);
        when(storageProvider.getNodeStatusesForOperation(OPERATION_ID)).thenReturn(statuses);
        when(operationalJobCoordinator.clearActive(OperationType.DRAIN, OPERATION_ID, null)).thenReturn(true);

        executeAndWait();

        verify(storageProvider, never()).updateJobStatus(any(), any(), any(), any());
    }

    @Test
    void testLocalNodeAlreadyAborted()
    {
        buildCoordinatorWithTestFactory(defaultConfig());
        OperationalJobRecord jobRecord = createJobRecord(Arrays.asList(
        Arrays.asList(NODE_1)
        ));
        setupSingleNodeInstance(NODE_1, "127.0.0.1");

        Map<UUID, OperationalJobStatus> statuses = new HashMap<>();
        statuses.put(NODE_1, OperationalJobStatus.ABORTED);

        when(storageProvider.isAvailable()).thenReturn(true);
        when(operationalJobCoordinator.getActiveOperations())
        .thenReturn(Collections.singletonMap(OperationType.DRAIN, OPERATION_ID));
        when(storageProvider.findJob(OPERATION_ID)).thenReturn(jobRecord);
        when(storageProvider.getNodeStatusesForOperation(OPERATION_ID)).thenReturn(statuses);

        executeAndWait();

        assertThat(localJobManager.getJob(OPERATION_ID, NODE_1)).isNull();
        verify(storageProvider, never()).updateNodeStatus(OPERATION_ID, NODE_1, OperationalJobStatus.RUNNING);
    }

    @Test
    void testAbortedJobRecordStopsFurtherSubmission()
    {
        buildCoordinatorWithTestFactory(defaultConfig());
        OperationalJobRecord jobRecord = createJobRecord(OperationType.DRAIN, OperationalJobStatus.ABORTED,
                                                         Arrays.asList(Arrays.asList(NODE_1)));
        setupSingleNodeInstance(NODE_1, "127.0.0.1");

        when(storageProvider.isAvailable()).thenReturn(true);
        when(operationalJobCoordinator.getActiveOperations())
        .thenReturn(Collections.singletonMap(OperationType.DRAIN, OPERATION_ID));
        when(storageProvider.findJob(OPERATION_ID)).thenReturn(jobRecord);
        when(storageProvider.getNodeStatusesForOperation(OPERATION_ID)).thenReturn(new HashMap<>());

        executeAndWait();

        verify(storageProvider, never()).updateNodeStatus(OPERATION_ID, NODE_1, OperationalJobStatus.RUNNING);
        assertThat(localJobManager.getJob(OPERATION_ID, NODE_1)).isNull();
    }

    @Test
    void testAbandonedOperationSettlesALocalNodeThatNeverStarted()
    {
        buildCoordinatorWithTestFactory(defaultConfig());
        OperationalJobRecord jobRecord = createJobRecord(OperationType.DRAIN, OperationalJobStatus.ABORTED,
                                                         Arrays.asList(Arrays.asList(NODE_1)));
        setupSingleNodeInstance(NODE_1, "127.0.0.1");

        Map<UUID, OperationalJobStatus> statuses = new HashMap<>();
        statuses.put(NODE_1, OperationalJobStatus.CREATED);

        when(storageProvider.isAvailable()).thenReturn(true);
        when(operationalJobCoordinator.getActiveOperations())
        .thenReturn(Collections.singletonMap(OperationType.DRAIN, OPERATION_ID));
        when(storageProvider.findJob(OPERATION_ID)).thenReturn(jobRecord);
        when(storageProvider.getNodeStatusesForOperation(OPERATION_ID)).thenReturn(statuses);
        when(operationalJobCoordinator.clearActive(OperationType.DRAIN, OPERATION_ID, null)).thenReturn(true);

        executeAndWait();

        // Nothing else would ever write this row: the node has no local job to report through.
        verify(storageProvider).updateNodeStatus(OPERATION_ID, NODE_1, OperationalJobStatus.ABORTED);
        verify(operationalJobCoordinator).clearActive(OperationType.DRAIN, OPERATION_ID, null);
    }

    @Test
    void testAbandonedOperationSettlesLocalNodesWithTheRecordStatus()
    {
        buildCoordinatorWithTestFactory(defaultConfig());
        OperationalJobRecord jobRecord = createJobRecord(OperationType.DRAIN, OperationalJobStatus.FAILED,
                                                         Arrays.asList(Arrays.asList(NODE_1)));
        setupSingleNodeInstance(NODE_1, "127.0.0.1");

        Map<UUID, OperationalJobStatus> statuses = new HashMap<>();
        statuses.put(NODE_1, OperationalJobStatus.CREATED);

        when(storageProvider.isAvailable()).thenReturn(true);
        when(operationalJobCoordinator.getActiveOperations())
        .thenReturn(Collections.singletonMap(OperationType.DRAIN, OPERATION_ID));
        when(storageProvider.findJob(OPERATION_ID)).thenReturn(jobRecord);
        when(storageProvider.getNodeStatusesForOperation(OPERATION_ID)).thenReturn(statuses);

        executeAndWait();

        verify(storageProvider).updateNodeStatus(OPERATION_ID, NODE_1, OperationalJobStatus.FAILED);
        verify(storageProvider, never()).updateNodeStatus(OPERATION_ID, NODE_1, OperationalJobStatus.ABORTED);
    }

    @Test
    void testAbandonedOperationDoesNotSettleNodesOwnedByOtherSidecars()
    {
        buildCoordinatorWithTestFactory(defaultConfig());
        OperationalJobRecord jobRecord = createJobRecord(OperationType.DRAIN, OperationalJobStatus.ABORTED,
                                                         Arrays.asList(Arrays.asList(NODE_1),
                                                                       Arrays.asList(NODE_2)));
        setupSingleNodeInstance(NODE_1, "127.0.0.1");

        Map<UUID, OperationalJobStatus> statuses = new HashMap<>();
        statuses.put(NODE_1, OperationalJobStatus.CREATED);
        statuses.put(NODE_2, OperationalJobStatus.CREATED);

        when(storageProvider.isAvailable()).thenReturn(true);
        when(operationalJobCoordinator.getActiveOperations())
        .thenReturn(Collections.singletonMap(OperationType.DRAIN, OPERATION_ID));
        when(storageProvider.findJob(OPERATION_ID)).thenReturn(jobRecord);
        when(storageProvider.getNodeStatusesForOperation(OPERATION_ID)).thenReturn(statuses);

        executeAndWait();

        verify(storageProvider).updateNodeStatus(OPERATION_ID, NODE_1, OperationalJobStatus.ABORTED);
        verify(storageProvider, never()).updateNodeStatus(eq(OPERATION_ID), eq(NODE_2), any());
        verify(operationalJobCoordinator, never()).clearActive(any(), any(), any());
    }

    @Test
    void testAbandonedOperationRecoversANodeLeftRunningByACrashedSidecar() throws InterruptedException
    {
        CountDownLatch recovered = new CountDownLatch(1);
        localJobFactory = (opId, nodeId, host, opType) -> new LocalJob(opId, nodeId, host, opType)
        {
            @Override
            protected void executeInternal()
            {
            }

            @Override
            public Future<Void> onJobFailed()
            {
                recovered.countDown();
                return Future.succeededFuture();
            }
        };
        buildCoordinator(defaultConfig());
        OperationalJobRecord jobRecord = createJobRecord(OperationType.DRAIN, OperationalJobStatus.ABORTED,
                                                         Arrays.asList(Arrays.asList(NODE_1)));
        setupSingleNodeInstance(NODE_1, "127.0.0.1");

        // RUNNING with no local job handle: the node started under a Sidecar process that has since gone away,
        // so no completion callback will ever report for it.
        Map<UUID, OperationalJobStatus> statuses = new HashMap<>();
        statuses.put(NODE_1, OperationalJobStatus.RUNNING);

        when(storageProvider.isAvailable()).thenReturn(true);
        when(operationalJobCoordinator.getActiveOperations())
        .thenReturn(Collections.singletonMap(OperationType.DRAIN, OPERATION_ID));
        when(storageProvider.findJob(OPERATION_ID)).thenReturn(jobRecord);
        when(storageProvider.getNodeStatusesForOperation(OPERATION_ID)).thenReturn(statuses);

        executeAndWait();

        verify(storageProvider).updateNodeStatus(OPERATION_ID, NODE_1, OperationalJobStatus.FAILED);
        verify(storageProvider, never()).updateNodeStatus(OPERATION_ID, NODE_1, OperationalJobStatus.ABORTED);
        assertThat(recovered.await(5, TimeUnit.SECONDS))
        .describedAs("a node left part-way through its operation must still be reconciled, or an aborted restart "
                     + "leaves Cassandra down with nothing trying to bring it back")
        .isTrue();
    }

    @Test
    void testLockSurvivesAbortUntilTheLocalJobStopsAndItsRowSettles() throws InterruptedException
    {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        localJobFactory = (opId, nodeId, host, opType) -> new LocalJob(opId, nodeId, host, opType)
        {
            @Override
            protected void executeInternal()
            {
                started.countDown();
                try
                {
                    assertThat(release.await(5, TimeUnit.SECONDS)).isTrue();
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                }
            }
        };
        buildCoordinator(defaultConfig());

        List<List<UUID>> executionOrder = Arrays.asList(Arrays.asList(NODE_1));
        OperationalJobRecord running = createJobRecord(OperationType.DRAIN, OperationalJobStatus.RUNNING,
                                                       executionOrder);
        OperationalJobRecord aborted = createJobRecord(OperationType.DRAIN, OperationalJobStatus.ABORTED,
                                                       executionOrder);
        setupSingleNodeInstance(NODE_1, "127.0.0.1");

        Map<UUID, OperationalJobStatus> statuses = new HashMap<>();

        when(storageProvider.isAvailable()).thenReturn(true);
        when(operationalJobCoordinator.getActiveOperations())
        .thenReturn(Collections.singletonMap(OperationType.DRAIN, OPERATION_ID));
        when(storageProvider.findJob(OPERATION_ID)).thenReturn(running, aborted);
        when(storageProvider.getNodeStatusesForOperation(OPERATION_ID)).thenReturn(statuses);
        when(operationalJobCoordinator.clearActive(OperationType.DRAIN, OPERATION_ID, null)).thenReturn(true);

        executeAndWait();
        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
        statuses.put(NODE_1, OperationalJobStatus.RUNNING);

        executeAndWait();

        // The job is still running, so its row must stay non-terminal and the lock must stay held. Releasing here
        // is what would let a new operation start alongside a restart that has not actually stopped.
        assertThat(localJobManager.getJob(OPERATION_ID, NODE_1)).isNotNull();
        verify(storageProvider, never()).updateNodeStatus(OPERATION_ID, NODE_1, OperationalJobStatus.ABORTED);
        verify(operationalJobCoordinator, never()).clearActive(any(), any(), any());

        release.countDown();
        verify(storageProvider, timeout(5000)).updateNodeStatus(OPERATION_ID, NODE_1,
                                                                OperationalJobStatus.SUCCEEDED);
        statuses.put(NODE_1, OperationalJobStatus.SUCCEEDED);

        executeAndWait();

        assertThat(localJobManager.getJob(OPERATION_ID, NODE_1)).isNull();
        verify(operationalJobCoordinator).clearActive(OperationType.DRAIN, OPERATION_ID, null);
    }

    @Test
    void testFinalizationReleasesLocksWhenAllNodesAborted()
    {
        buildCoordinatorWithTestFactory(defaultConfig());
        OperationalJobRecord jobRecord = createJobRecord(OperationType.DRAIN, OperationalJobStatus.ABORTED,
                                                         Arrays.asList(Arrays.asList(NODE_1)));
        setupSingleNodeInstance(NODE_1, "127.0.0.1");

        Map<UUID, OperationalJobStatus> statuses = new HashMap<>();
        statuses.put(NODE_1, OperationalJobStatus.ABORTED);

        when(storageProvider.isAvailable()).thenReturn(true);
        when(operationalJobCoordinator.getActiveOperations())
        .thenReturn(Collections.singletonMap(OperationType.DRAIN, OPERATION_ID));
        when(storageProvider.findJob(OPERATION_ID)).thenReturn(jobRecord);
        when(storageProvider.getNodeStatusesForOperation(OPERATION_ID)).thenReturn(statuses);
        when(operationalJobCoordinator.clearActive(OperationType.DRAIN, OPERATION_ID, null)).thenReturn(true);

        executeAndWait();

        verify(operationalJobCoordinator).clearActive(OperationType.DRAIN, OPERATION_ID, null);
        verify(storageProvider, never()).updateJobStatus(any(), any(), any(), any());
    }

    @Test
    void testAbortedNodesDoNotFinalizeAsSucceeded()
    {
        buildCoordinatorWithTestFactory(defaultConfig());
        OperationalJobRecord jobRecord = createJobRecord(Arrays.asList(
        Arrays.asList(NODE_1)
        ));
        setupSingleNodeInstance(NODE_1, "127.0.0.1");

        Map<UUID, OperationalJobStatus> statuses = new HashMap<>();
        statuses.put(NODE_1, OperationalJobStatus.ABORTED);

        when(storageProvider.isAvailable()).thenReturn(true);
        when(operationalJobCoordinator.getActiveOperations())
        .thenReturn(Collections.singletonMap(OperationType.DRAIN, OPERATION_ID));
        when(storageProvider.findJob(OPERATION_ID)).thenReturn(jobRecord);
        when(storageProvider.getNodeStatusesForOperation(OPERATION_ID)).thenReturn(statuses);
        when(operationalJobCoordinator.clearActive(OperationType.DRAIN, OPERATION_ID, null)).thenReturn(true);

        executeAndWait();

        verify(storageProvider).updateJobStatus(eq(OPERATION_ID), eq(OperationType.DRAIN),
                                                eq(OperationalJobStatus.FAILED), any());
    }

    @Test
    void testAbortedPredecessorCascadesAbortedRatherThanFailed()
    {
        localJobFactory = predecessorSuccessTestFactory();
        buildCoordinator(defaultConfig());
        OperationalJobRecord jobRecord = createJobRecord(Arrays.asList(
        Arrays.asList(NODE_1),
        Arrays.asList(NODE_2)
        ));
        setupSingleNodeInstance(NODE_2, "127.0.0.2");

        Map<UUID, OperationalJobStatus> statuses = new HashMap<>();
        statuses.put(NODE_1, OperationalJobStatus.ABORTED);

        when(storageProvider.isAvailable()).thenReturn(true);
        when(operationalJobCoordinator.getActiveOperations())
        .thenReturn(Collections.singletonMap(OperationType.DRAIN, OPERATION_ID));
        when(storageProvider.findJob(OPERATION_ID)).thenReturn(jobRecord);
        when(storageProvider.getNodeStatusesForOperation(OPERATION_ID)).thenReturn(statuses);

        executeAndWait();

        // Guards against collapsing the cascade back onto a FAILED-only check, which would leave the successor
        // waiting forever on a predecessor that can never succeed.
        verify(storageProvider).updateNodeStatus(OPERATION_ID, NODE_2, OperationalJobStatus.ABORTED);
        verify(storageProvider, never()).updateNodeStatus(OPERATION_ID, NODE_2, OperationalJobStatus.RUNNING);
    }

    @Test
    void testNoActiveOperationsCancelsLeftoverLocalJobs() throws InterruptedException
    {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch cancelObserved = new CountDownLatch(1);
        localJobFactory = (opId, nodeId, host, opType) -> new LocalJob(opId, nodeId, host, opType)
        {
            @Override
            protected void executeInternal()
            {
                started.countDown();
                while (!isCancelled())
                {
                    try
                    {
                        Thread.sleep(20);
                    }
                    catch (InterruptedException e)
                    {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
                cancelObserved.countDown();
            }
        };
        buildCoordinator(defaultConfig());

        OperationalJobRecord jobRecord = createJobRecord(Arrays.asList(Arrays.asList(NODE_1)));
        setupSingleNodeInstance(NODE_1, "127.0.0.1");

        when(storageProvider.isAvailable()).thenReturn(true);
        when(operationalJobCoordinator.getActiveOperations())
        .thenReturn(Collections.singletonMap(OperationType.DRAIN, OPERATION_ID), Collections.emptyMap());
        when(storageProvider.findJob(OPERATION_ID)).thenReturn(jobRecord);
        when(storageProvider.getNodeStatusesForOperation(OPERATION_ID)).thenReturn(new HashMap<>());

        executeAndWait();
        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();

        executeAndWait();

        assertThat(cancelObserved.await(5, TimeUnit.SECONDS))
        .describedAs("Resetting state must cancel leftover jobs, not just drop their handles")
        .isTrue();
        assertThat(localJobManager.getJob(OPERATION_ID, NODE_1)).isNull();
    }

    @Test
    void testNodeIsNotSettledWhileItsLocalJobStillOwesARowWrite() throws Exception
    {
        buildCoordinatorWithTestFactory(defaultConfig());
        OperationalJobRecord jobRecord = createJobRecord(OperationType.DRAIN, OperationalJobStatus.ABORTED,
                                                         Arrays.asList(Arrays.asList(NODE_1)));
        setupSingleNodeInstance(NODE_1, "127.0.0.1");

        // A job that has run to completion but whose terminal row this Sidecar has not written yet. Submitting it
        // through the manager alone reproduces that window: the handle is tracked and its status is already
        // SUCCEEDED, while storage still reads RUNNING.
        LocalJob finishedJob = new LocalJob(OPERATION_ID, NODE_1, "127.0.0.1", OperationType.DRAIN)
        {
            @Override
            protected void executeInternal()
            {
            }
        };
        localJobManager.submitJob(finishedJob).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        assertThat(finishedJob.status()).isEqualTo(OperationalJobStatus.SUCCEEDED);

        Map<UUID, OperationalJobStatus> statuses = new HashMap<>();
        statuses.put(NODE_1, OperationalJobStatus.RUNNING);

        when(storageProvider.isAvailable()).thenReturn(true);
        when(operationalJobCoordinator.getActiveOperations())
        .thenReturn(Collections.singletonMap(OperationType.DRAIN, OPERATION_ID));
        when(storageProvider.findJob(OPERATION_ID)).thenReturn(jobRecord);
        when(storageProvider.getNodeStatusesForOperation(OPERATION_ID)).thenReturn(statuses);

        executeAndWait();

        // The node succeeded. Settling it here writes FAILED over an outcome this Sidecar is about to report
        // correctly, and hands a healthy node a recovery it does not need.
        verify(storageProvider, never()).updateNodeStatus(OPERATION_ID, NODE_1, OperationalJobStatus.FAILED);
    }

    @Test
    void testPollDischargesARowWriteItsFinishedJobNeverLanded() throws Exception
    {
        buildCoordinatorWithTestFactory(defaultConfig());
        OperationalJobRecord jobRecord = createJobRecord(OperationType.DRAIN, OperationalJobStatus.ABORTED,
                                                         Arrays.asList(Arrays.asList(NODE_1)));
        setupSingleNodeInstance(NODE_1, "127.0.0.1");

        LocalJob finishedJob = new LocalJob(OPERATION_ID, NODE_1, "127.0.0.1", OperationType.DRAIN)
        {
            @Override
            protected void executeInternal()
            {
            }
        };
        localJobManager.submitJob(finishedJob).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);

        Map<UUID, OperationalJobStatus> statuses = new HashMap<>();
        statuses.put(NODE_1, OperationalJobStatus.RUNNING);

        when(storageProvider.isAvailable()).thenReturn(true);
        when(operationalJobCoordinator.getActiveOperations())
        .thenReturn(Collections.singletonMap(OperationType.DRAIN, OPERATION_ID));
        when(storageProvider.findJob(OPERATION_ID)).thenReturn(jobRecord);
        when(storageProvider.getNodeStatusesForOperation(OPERATION_ID)).thenReturn(statuses);

        executeAndWait();

        // The job is done and its row never reached storage, so nothing else will ever write it. Skipping the node
        // on the handle alone would leave the row non-terminal, the lock held, and the handle uncollectable,
        // because resetState only runs once no operation is active.
        verify(storageProvider).updateNodeStatus(OPERATION_ID, NODE_1, OperationalJobStatus.SUCCEEDED);
        assertThat(localJobManager.getJob(OPERATION_ID, NODE_1))
        .describedAs("the debt is paid, so the handle must not keep blocking this node")
        .isNull();
    }

    @Test
    void testLockIsHeldWhenSettlingANodeRowFailsToWrite()
    {
        buildCoordinatorWithTestFactory(defaultConfig());
        OperationalJobRecord jobRecord = createJobRecord(OperationType.DRAIN, OperationalJobStatus.ABORTED,
                                                         Arrays.asList(Arrays.asList(NODE_1)));
        setupSingleNodeInstance(NODE_1, "127.0.0.1");

        Map<UUID, OperationalJobStatus> statuses = new HashMap<>();
        statuses.put(NODE_1, OperationalJobStatus.CREATED);

        when(storageProvider.isAvailable()).thenReturn(true);
        when(operationalJobCoordinator.getActiveOperations())
        .thenReturn(Collections.singletonMap(OperationType.DRAIN, OPERATION_ID));
        when(storageProvider.findJob(OPERATION_ID)).thenReturn(jobRecord);
        when(storageProvider.getNodeStatusesForOperation(OPERATION_ID)).thenReturn(statuses);
        doThrow(new RuntimeException("Cassandra unavailable"))
        .when(storageProvider).updateNodeStatus(OPERATION_ID, NODE_1, OperationalJobStatus.ABORTED);

        executeAndWait();

        verify(operationalJobCoordinator, never()).clearActive(any(), any(), any());
    }

    @Test
    void testRecoveryReportsTheOutcomeOfOnJobFailed() throws Exception
    {
        buildCoordinator(defaultConfig());
        RuntimeException recoveryFailure = new RuntimeException("could not bring the node back up");
        LocalJob job = new LocalJob(OPERATION_ID, NODE_1, "127.0.0.1", OperationType.DRAIN)
        {
            @Override
            protected void executeInternal()
            {
            }

            @Override
            public Future<Void> onJobFailed()
            {
                return Future.failedFuture(recoveryFailure);
            }
        };

        Future<Void> recovery = coordinator.recoverFailedNode(job, OPERATION_ID, NODE_1);

        assertThat(recovery.toCompletionStage().toCompletableFuture())
        .failsWithin(5, TimeUnit.SECONDS)
        .withThrowableThat()
        .withCause(recoveryFailure);
    }

    private OperationalJobConfiguration defaultConfig()
    {
        return new OperationalJobConfigurationImpl();
    }

    private void buildCoordinator(OperationalJobConfiguration config)
    {
        SidecarConfiguration sidecarConfig = mock(SidecarConfiguration.class);
        when(sidecarConfig.operationalJobConfiguration()).thenReturn(config);
        if (localJobFactory == null)
        {
            localJobFactory = defaultTestFactory();
        }
        coordinator = new StorageBackedLocalJobCoordinator(executorPools,
                                                           operationalJobCoordinator,
                                                           storageProvider,
                                                           instanceMetadataFetcher,
                                                           localJobManager,
                                                           sidecarConfig,
                                                           localJobFactory,
                                                           timeProvider);
    }

    /**
     * Lets the settle-window tests advance time without sleeping.
     */
    static class FakeTimeProvider implements TimeProvider
    {
        private final AtomicLong nanos = new AtomicLong(0);

        @Override
        public long currentTimeMillis()
        {
            return TimeUnit.NANOSECONDS.toMillis(nanos.get());
        }

        @Override
        public long nanoTime()
        {
            return nanos.get();
        }

        void advance(long value, TimeUnit unit)
        {
            nanos.addAndGet(unit.toNanos(value));
        }
    }

    private void buildCoordinatorWithTestFactory(OperationalJobConfiguration config)
    {
        localJobFactory = defaultTestFactory();
        buildCoordinator(config);
    }

    private LocalJobFactory defaultTestFactory()
    {
        return (opId, nodeId, host, opType) -> new LocalJob(opId, nodeId, host, opType)
        {
            @Override
            protected void executeInternal()
            {
            }
        };
    }

    private LocalJobFactory predecessorSuccessTestFactory()
    {
        return new LocalJobFactory()
        {
            @Override
            public LocalJob createJob(UUID opId, UUID nodeId, String host, OperationType opType)
            {
                return new LocalJob(opId, nodeId, host, opType)
                {
                    @Override
                    protected void executeInternal()
                    {
                    }
                };
            }

            @Override
            public boolean requiresPredecessorSuccess()
            {
                return true;
            }
        };
    }

    private LocalJobFactory waitingTestFactory(String waitBetweenExecutionGroups)
    {
        return new LocalJobFactory()
        {
            @Override
            public LocalJob createJob(UUID opId, UUID nodeId, String host, OperationType opType)
            {
                return new LocalJob(opId, nodeId, host, opType)
                {
                    @Override
                    protected void executeInternal()
                    {
                    }
                };
            }

            @Override
            public SecondBoundConfiguration waitBetweenExecutionGroups(Map<String, String> operationMetadata)
            {
                return SecondBoundConfiguration.parse(waitBetweenExecutionGroups);
            }
        };
    }

    private OperationalJobRecord createJobRecord(List<List<UUID>> nodeExecutionOrder)
    {
        return createJobRecord(OperationType.DRAIN, nodeExecutionOrder);
    }

    private OperationalJobRecord createJobRecord(OperationType operationType, List<List<UUID>> nodeExecutionOrder)
    {
        return createJobRecord(operationType, OperationalJobStatus.RUNNING, nodeExecutionOrder);
    }

    private OperationalJobRecord createJobRecord(OperationType operationType,
                                                 OperationalJobStatus status,
                                                 List<List<UUID>> nodeExecutionOrder)
    {
        return OperationalJobRecord.builder()
                                   .jobId(OPERATION_ID)
                                   .operationType(operationType)
                                   .status(status)
                                   .nodeExecutionOrder(nodeExecutionOrder)
                                   .build();
    }

    private void setupSingleNodeInstance(UUID nodeId, String host)
    {
        InstanceMetadata instance = mock(InstanceMetadata.class);
        CassandraAdapterDelegate delegate = mock(CassandraAdapterDelegate.class);
        NodeSettings nodeSettings = NodeSettings.builder().hostId(nodeId).build();

        when(instance.host()).thenReturn(host);
        when(instance.delegate()).thenReturn(delegate);
        when(delegate.nodeSettings()).thenReturn(nodeSettings);
        when(instanceMetadataFetcher.allLocalInstances()).thenReturn(Collections.singletonList(instance));
    }

    private void setupMultipleNodeInstances(UUID[] nodeIds, String[] hosts)
    {
        List<InstanceMetadata> instances = new java.util.ArrayList<>();
        for (int i = 0; i < nodeIds.length; i++)
        {
            InstanceMetadata instance = mock(InstanceMetadata.class);
            CassandraAdapterDelegate delegate = mock(CassandraAdapterDelegate.class);
            NodeSettings nodeSettings = NodeSettings.builder().hostId(nodeIds[i]).build();

            when(instance.host()).thenReturn(hosts[i]);
            when(instance.delegate()).thenReturn(delegate);
            when(delegate.nodeSettings()).thenReturn(nodeSettings);
            instances.add(instance);
        }
        when(instanceMetadataFetcher.allLocalInstances()).thenReturn(instances);
    }

    private void executeAndWait()
    {
        CountDownLatch latch = new CountDownLatch(1);
        Promise<Void> promise = Promise.promise();
        promise.future().onComplete(ar -> latch.countDown());
        coordinator.execute(promise);
        try
        {
            assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }
}
