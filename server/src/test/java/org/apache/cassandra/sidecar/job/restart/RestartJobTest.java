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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.common.data.Lifecycle.CassandraState;
import org.apache.cassandra.sidecar.common.data.Lifecycle.OperationStatus;
import org.apache.cassandra.sidecar.common.data.OperationType;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.apache.cassandra.sidecar.common.response.LifecycleInfoResponse;
import org.apache.cassandra.sidecar.common.response.RingResponse;
import org.apache.cassandra.sidecar.common.response.data.RingEntry;
import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.RollingRestartConfiguration;
import org.apache.cassandra.sidecar.config.yaml.RollingRestartConfigurationImpl;
import org.apache.cassandra.sidecar.exceptions.LifecycleTaskConflictException;
import org.apache.cassandra.sidecar.lifecycle.LifecycleManager;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link RestartJob}.
 * <p>
 * Lifecycle transitions are modelled with a small stateful fake ({@link #stubLifecycle}) tracking the node's
 * current and desired state.
 */
class RestartJobTest
{
    private LifecycleManager lifecycleManager;
    private RestartHealthChecker healthChecker;
    private RollingRestartConfiguration config;
    private UUID operationId;
    private UUID nodeId;
    private String instanceHost;

    // Stateful lifecycle model shared by the fake stubs.
    private final AtomicReference<CassandraState> currentState = new AtomicReference<>();
    private final AtomicReference<CassandraState> desiredState = new AtomicReference<>();

    @BeforeEach
    void setup()
    {
        lifecycleManager = mock(LifecycleManager.class);
        healthChecker = mock(RestartHealthChecker.class);
        config = RollingRestartConfigurationImpl.builder()
                                               .enabled(true)
                                               .cassandraHealthTimeout(SecondBoundConfiguration.parse("2s"))
                                               .nodeStateTransitionTimeout(SecondBoundConfiguration.parse("2s"))
                                               .nodeRestartRetryAttempts(3)
                                               .build();
        operationId = UUID.randomUUID();
        nodeId = UUID.randomUUID();
        instanceHost = "127.0.0.1";
    }

    @Test
    void testSuccessfulRestart() throws Exception
    {
        when(healthChecker.isHealthy(any(), eq(nodeId))).thenReturn(true);
        stubLifecycle(CassandraState.RUNNING);

        RestartJob job = createJob();
        job.execute();
        assertThat(job.status()).isEqualTo(OperationalJobStatus.SUCCEEDED);
        // A running node is stopped then started.
        verify(lifecycleManager).updateDesiredState(eq(instanceHost), eq(CassandraState.STOPPED));
        verify(lifecycleManager).updateDesiredState(eq(instanceHost), eq(CassandraState.RUNNING));
    }

    @Test
    void testAlreadyDownStartsWithoutHealthCheckOrStop() throws Exception
    {
        // Node is already down: the health check and stop must be skipped, and it is started directly.
        stubLifecycle(CassandraState.STOPPED);

        RestartJob job = createJob();
        job.execute();
        assertThat(job.status()).isEqualTo(OperationalJobStatus.SUCCEEDED);
        verify(healthChecker, never()).isHealthy(any(), any());
        verify(lifecycleManager, never()).updateDesiredState(any(), eq(CassandraState.STOPPED));
        verify(lifecycleManager, times(1)).updateDesiredState(eq(instanceHost), eq(CassandraState.RUNNING));
    }

    @Test
    void testRingHealthCheckFailsTimesOut() throws Exception
    {
        when(healthChecker.isHealthy(any(), eq(nodeId))).thenReturn(false);
        stubLifecycle(CassandraState.RUNNING);

        RestartJob job = createJob();
        try
        {
            job.execute();
        }
        catch (Exception ignored)
        {
        }
        assertThat(job.status()).isEqualTo(OperationalJobStatus.FAILED);
        // Never transitions state when the ring stays unhealthy.
        verify(lifecycleManager, never()).updateDesiredState(any(), any());
    }

    @Test
    void testStopFailsRetriesAndSucceeds() throws Exception
    {
        when(healthChecker.isHealthy(any(), eq(nodeId))).thenReturn(true);
        stubLifecycle(CassandraState.RUNNING);

        AtomicInteger stopCallCount = new AtomicInteger(0);
        when(lifecycleManager.updateDesiredState(eq(instanceHost), eq(CassandraState.STOPPED)))
            .thenAnswer(inv -> {
                if (stopCallCount.incrementAndGet() == 1)
                {
                    throw new LifecycleTaskConflictException("Task already in progress");
                }
                converge(CassandraState.STOPPED);
                return info();
            });

        RestartJob job = createJob();
        job.execute();
        assertThat(job.status()).isEqualTo(OperationalJobStatus.SUCCEEDED);
        assertThat(stopCallCount.get()).isEqualTo(2);
    }

    @Test
    void testAllRetriesExhausted() throws Exception
    {
        when(healthChecker.isHealthy(any(), eq(nodeId))).thenReturn(true);
        stubLifecycle(CassandraState.RUNNING);
        when(lifecycleManager.updateDesiredState(eq(instanceHost), eq(CassandraState.STOPPED)))
            .thenThrow(new RuntimeException("Stop failed"));

        RestartJob job = createJob();
        try
        {
            job.execute();
        }
        catch (Exception ignored)
        {
        }
        assertThat(job.status()).isEqualTo(OperationalJobStatus.FAILED);
        // nodeRestartRetryAttempts=3 counts retries, so there are 4 total attempts (initial + 3 retries).
        verify(lifecycleManager, times(4)).updateDesiredState(eq(instanceHost), eq(CassandraState.STOPPED));
    }

    @Test
    void testHealthCheckBeforeEachRetry() throws Exception
    {
        AtomicInteger healthCheckCount = new AtomicInteger(0);
        when(healthChecker.isHealthy(any(), eq(nodeId))).thenAnswer(inv -> {
            healthCheckCount.incrementAndGet();
            return true;
        });
        stubLifecycle(CassandraState.RUNNING);
        when(lifecycleManager.updateDesiredState(eq(instanceHost), eq(CassandraState.STOPPED)))
            .thenThrow(new RuntimeException("Stop failed"));

        RestartJob job = createJob();
        try
        {
            job.execute();
        }
        catch (Exception ignored)
        {
        }
        // nodeRestartRetryAttempts=3 counts retries, so there are 4 total attempts, each preceded by a health check.
        assertThat(healthCheckCount.get()).isEqualTo(4);
    }

    @Test
    void testStartFailsRetriesAndSucceeds() throws Exception
    {
        when(healthChecker.isHealthy(any(), eq(nodeId))).thenReturn(true);
        stubLifecycle(CassandraState.RUNNING);

        // First start attempt diverges (node stays STOPPED); the node is then already down on the next
        // attempt and is started directly, which succeeds.
        AtomicInteger startCallCount = new AtomicInteger(0);
        when(lifecycleManager.updateDesiredState(eq(instanceHost), eq(CassandraState.RUNNING)))
            .thenAnswer(inv -> {
                desiredState.set(CassandraState.RUNNING);
                if (startCallCount.incrementAndGet() == 1)
                {
                    currentState.set(CassandraState.STOPPED);
                    return new LifecycleInfoResponse(CassandraState.STOPPED, CassandraState.RUNNING,
                                                     OperationStatus.DIVERGED, "test");
                }
                converge(CassandraState.RUNNING);
                return info();
            });
        // getLifecycleInfo reports DIVERGED while the first start is pending, so updateLifecycleStateAndWait returns fast.
        when(lifecycleManager.getLifecycleInfo(instanceHost)).thenAnswer(inv -> {
            if (startCallCount.get() == 1
                && currentState.get() == CassandraState.STOPPED
                && desiredState.get() == CassandraState.RUNNING)
            {
                return new LifecycleInfoResponse(CassandraState.STOPPED, CassandraState.RUNNING,
                                                 OperationStatus.DIVERGED, "test");
            }
            return info();
        });

        RestartJob job = createJob();
        job.execute();
        assertThat(job.status()).isEqualTo(OperationalJobStatus.SUCCEEDED);
        assertThat(startCallCount.get()).isEqualTo(2);
    }

    @Test
    void testOnJobFailedRestartsStoppedNode() throws Exception
    {
        when(lifecycleManager.getLifecycleInfo(instanceHost))
            .thenReturn(new LifecycleInfoResponse(CassandraState.STOPPED, CassandraState.STOPPED,
                                                  OperationStatus.CONVERGED, "test"));
        when(lifecycleManager.updateDesiredState(eq(instanceHost), eq(CassandraState.RUNNING)))
            .thenReturn(new LifecycleInfoResponse(CassandraState.STOPPED, CassandraState.RUNNING,
                                                  OperationStatus.CONVERGING, "test"));

        RestartJob job = createJob();
        job.onJobFailed();
        verify(lifecycleManager).updateDesiredState(eq(instanceHost), eq(CassandraState.RUNNING));
    }

    @Test
    void testOnJobFailedNoOpIfNodeRunning() throws Exception
    {
        when(lifecycleManager.getLifecycleInfo(instanceHost))
            .thenReturn(new LifecycleInfoResponse(CassandraState.RUNNING, CassandraState.RUNNING,
                                                  OperationStatus.CONVERGED, "test"));

        RestartJob job = createJob();
        job.onJobFailed();
        verify(lifecycleManager, never()).updateDesiredState(any(), any());
    }

    @Test
    void testOnJobFailedStartsNodeWhoseStopHasNotConvergedYet() throws Exception
    {
        when(lifecycleManager.getLifecycleInfo(instanceHost))
            .thenReturn(new LifecycleInfoResponse(CassandraState.RUNNING, CassandraState.STOPPED,
                                                  OperationStatus.CONVERGING, "test"));

        RestartJob job = createJob();
        job.onJobFailed();

        verify(lifecycleManager).updateDesiredState(eq(instanceHost), eq(CassandraState.RUNNING));
    }

    @Test
    void testOnJobFailedRetriesWhileTheStopItIsRecoveringFromIsStillInFlight() throws Exception
    {
        when(lifecycleManager.getLifecycleInfo(instanceHost))
            .thenReturn(new LifecycleInfoResponse(CassandraState.STOPPED, CassandraState.STOPPED,
                                                  OperationStatus.CONVERGED, "test"));
        // The lifecycle manager admits one task per host at a time, so the start is rejected until the stop's
        // async cleanup completes. Giving up on the first conflict would leave the node down.
        AtomicInteger attempts = new AtomicInteger();
        when(lifecycleManager.updateDesiredState(eq(instanceHost), eq(CassandraState.RUNNING)))
            .thenAnswer(inv -> {
                if (attempts.incrementAndGet() < 3)
                {
                    throw new LifecycleTaskConflictException("Task already in progress for this host.");
                }
                return new LifecycleInfoResponse(CassandraState.RUNNING, CassandraState.RUNNING,
                                                 OperationStatus.CONVERGING, "test");
            });

        RestartJob job = createJob();
        job.onJobFailed();

        assertThat(attempts.get())
        .describedAs("recovery must retry the conflict rather than abandon the node")
        .isEqualTo(3);
        verify(lifecycleManager, times(3)).updateDesiredState(eq(instanceHost), eq(CassandraState.RUNNING));
    }


    @Test
    void testCancelBeforeExecuteSkipsAllWork() throws Exception
    {
        stubLifecycle(CassandraState.RUNNING);

        RestartJob job = createJob();
        job.cancel();
        try
        {
            job.execute();
        }
        catch (Exception ignored)
        {
        }
        assertThat(job.status()).isEqualTo(OperationalJobStatus.FAILED);
        verify(lifecycleManager, never()).updateDesiredState(any(), any());
    }

    @Test
    void testCancelWhileStoppingStillBringsTheNodeBackUp() throws Exception
    {
        stubLifecycle(CassandraState.RUNNING);
        when(healthChecker.isHealthy(any(), eq(nodeId))).thenReturn(true);

        RestartJob job = createJob();
        when(lifecycleManager.updateDesiredState(eq(instanceHost), eq(CassandraState.STOPPED)))
            .thenAnswer(inv -> {
                converge(CassandraState.STOPPED);
                job.cancel();
                return info();
            });

        job.execute();

        assertThat(job.status())
        .describedAs("a restart cancelled mid-stop must run to completion rather than strand the node")
        .isEqualTo(OperationalJobStatus.SUCCEEDED);
        verify(lifecycleManager).updateDesiredState(eq(instanceHost), eq(CassandraState.RUNNING));
        assertThat(currentState.get()).isEqualTo(CassandraState.RUNNING);
    }

    @Test
    void testCancelWhileStartingStillBringsTheNodeBackUp() throws Exception
    {
        stubLifecycle(CassandraState.RUNNING);
        when(healthChecker.isHealthy(any(), eq(nodeId))).thenReturn(true);

        RestartJob job = createJob();
        AtomicInteger pollsSinceStartRequested = new AtomicInteger(-1);
        when(lifecycleManager.getLifecycleInfo(instanceHost)).thenAnswer(inv -> {
            if (pollsSinceStartRequested.get() >= 0 && pollsSinceStartRequested.incrementAndGet() >= 2)
            {
                currentState.set(CassandraState.RUNNING);
            }
            return info();
        });
        when(lifecycleManager.updateDesiredState(eq(instanceHost), eq(CassandraState.STOPPED)))
            .thenAnswer(inv -> {
                converge(CassandraState.STOPPED);
                return info();
            });
        when(lifecycleManager.updateDesiredState(eq(instanceHost), eq(CassandraState.RUNNING)))
            .thenAnswer(inv -> {
                desiredState.set(CassandraState.RUNNING);
                pollsSinceStartRequested.set(0);
                job.cancel();
                return info();
            });

        job.execute();

        assertThat(job.status()).isEqualTo(OperationalJobStatus.SUCCEEDED);
        assertThat(currentState.get()).isEqualTo(CassandraState.RUNNING);
    }

    @Test
    void testCancelBeforeTheStopIsRequestedLeavesTheNodeRunning() throws Exception
    {
        stubLifecycle(CassandraState.RUNNING);

        RestartJob job = createJob();
        // Cancelled after the health check passes but before the stop is requested: the node has not been
        // touched, so the restart must abandon rather than take it down.
        when(healthChecker.isHealthy(any(), eq(nodeId))).thenAnswer(inv -> {
            job.cancel();
            return true;
        });
        try
        {
            job.execute();
        }
        catch (Exception ignored)
        {
        }

        assertThat(job.status()).isEqualTo(OperationalJobStatus.FAILED);
        verify(lifecycleManager, never()).updateDesiredState(any(), eq(CassandraState.STOPPED));
        assertThat(currentState.get())
        .describedAs("a node that was never stopped must be left running")
        .isEqualTo(CassandraState.RUNNING);
    }

    @Test
    void testCancelDuringRingHealthCheckStopsRestartPromptly() throws Exception
    {
        stubLifecycle(CassandraState.RUNNING);

        RestartJob job = createJob();
        // Cancel from inside the health-check poll loop. The job must bail out of the loop at the next
        // check rather than spinning until the health timeout, and must never stop the node.
        when(healthChecker.isHealthy(any(), eq(nodeId))).thenAnswer(inv -> {
            job.cancel();
            return false;
        });
        try
        {
            job.execute();
        }
        catch (Exception ignored)
        {
        }
        assertThat(job.status()).isEqualTo(OperationalJobStatus.FAILED);
        verify(lifecycleManager, never()).updateDesiredState(any(), eq(CassandraState.STOPPED));
        // Loop bailed right after cancel — a single health probe, not spinning until the 2s timeout.
        verify(healthChecker, times(1)).isHealthy(any(), eq(nodeId));
    }

    private RestartJob createJob()
    {
        RestartJob job = new RestartJob(operationId, nodeId, instanceHost, OperationType.RESTART,
                                       lifecycleManager, healthChecker,
                                       config.nodeRestartRetryAttempts(),
                                       config.cassandraHealthTimeout(),
                                       config.nodeStateTransitionTimeout(),
                                       this::createRingResponse);
        // Fast poll intervals keep the tests quick.
        job.setPollIntervalsForTesting(10, 10);
        return job;
    }

    /**
     * Installs a stateful lifecycle fake starting in {@code initial} state. {@code getLifecycleInfo} reflects
     * the tracked current/desired state, and {@code updateDesiredState} converges the node to the requested
     * state immediately. Individual tests override specific transitions to model conflicts/divergence.
     */
    private void stubLifecycle(CassandraState initial) throws Exception
    {
        currentState.set(initial);
        desiredState.set(initial);
        when(lifecycleManager.getLifecycleInfo(instanceHost)).thenAnswer(inv -> info());
        when(lifecycleManager.updateDesiredState(eq(instanceHost), any())).thenAnswer(inv -> {
            converge(inv.getArgument(1));
            return info();
        });
    }

    private void converge(CassandraState state)
    {
        desiredState.set(state);
        currentState.set(state);
    }

    private LifecycleInfoResponse info()
    {
        OperationStatus status = currentState.get() == desiredState.get()
                                 ? OperationStatus.CONVERGED : OperationStatus.CONVERGING;
        return new LifecycleInfoResponse(currentState.get(), desiredState.get(), status, "test");
    }

    private RingResponse createRingResponse()
    {
        RingResponse ring = new RingResponse();
        ring.add(new RingEntry.Builder()
                 .datacenter("dc1").rack("rackA").status("Up")
                 .address("127.0.0.1").port(9042).token("0")
                 .state("Normal").load("100 KiB").owns("").fqdn("localhost")
                 .hostId(UUID.randomUUID().toString()).build());
        ring.add(new RingEntry.Builder()
                 .datacenter("dc1").rack("rackB").status("Up")
                 .address("127.0.0.2").port(9042).token("1000")
                 .state("Normal").load("100 KiB").owns("").fqdn("localhost")
                 .hostId(UUID.randomUUID().toString()).build());
        return ring;
    }
}
