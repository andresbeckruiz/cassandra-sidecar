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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.common.data.OperationType;
import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.RollingRestartConfiguration;
import org.apache.cassandra.sidecar.config.yaml.RollingRestartConfigurationImpl;
import org.apache.cassandra.sidecar.job.LocalJob;
import org.apache.cassandra.sidecar.lifecycle.LifecycleManager;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

/**
 * Tests {@link RestartLocalJobFactory}: resolving per-request overrides from the operation metadata (falling back
 * to the configured yaml defaults when a field is absent or malformed), creating correctly-wired {@link RestartJob}
 * instances, and deriving the per-node execution timeout.
 */
class RestartLocalJobFactoryTest
{
    private final RollingRestartConfiguration config = RollingRestartConfigurationImpl.builder()
                                                       .enabled(true)
                                                       .cassandraHealthTimeout(SecondBoundConfiguration.parse("60s"))
                                                       .nodeStateTransitionTimeout(SecondBoundConfiguration.parse("600s"))
                                                       .nodeRestartRetryAttempts(3)
                                                       .waitBetweenExecutionGroups(SecondBoundConfiguration.parse("45s"))
                                                       .build();

    private final InstanceMetadataFetcher metadataFetcher = mock(InstanceMetadataFetcher.class);
    private final LifecycleManager lifecycleManager = mock(LifecycleManager.class);
    private final RestartHealthChecker healthChecker = mock(RestartHealthChecker.class);

    private RestartLocalJobFactory newFactory()
    {
        return new RestartLocalJobFactory(metadataFetcher, lifecycleManager, healthChecker, config);
    }

    @Test
    void testOverridesTakePrecedence()
    {
        Map<String, String> metadata = new HashMap<>();
        metadata.put(RestartOperationMetadata.NODE_RESTART_RETRY_ATTEMPTS, "7");
        metadata.put(RestartOperationMetadata.CASSANDRA_HEALTH_TIMEOUT_SECONDS, "120");
        metadata.put(RestartOperationMetadata.NODE_STATE_TRANSITION_TIMEOUT_SECONDS, "300");

        assertThat(RestartLocalJobFactory.resolveRetryAttempts(metadata, config)).isEqualTo(7);
        assertThat(RestartLocalJobFactory.resolveHealthTimeout(metadata, config).toMillis())
        .isEqualTo(SecondBoundConfiguration.parse("120s").toMillis());
        assertThat(RestartLocalJobFactory.resolveNodeStateTransitionTimeout(metadata, config).toMillis())
        .isEqualTo(SecondBoundConfiguration.parse("300s").toMillis());
    }

    @Test
    void testNullMetadataFallsBackToConfig()
    {
        assertThat(RestartLocalJobFactory.resolveRetryAttempts(null, config)).isEqualTo(3);
        assertThat(RestartLocalJobFactory.resolveHealthTimeout(null, config).toMillis())
        .isEqualTo(SecondBoundConfiguration.parse("60s").toMillis());
        assertThat(RestartLocalJobFactory.resolveNodeStateTransitionTimeout(null, config).toMillis())
        .isEqualTo(SecondBoundConfiguration.parse("600s").toMillis());
    }

    @Test
    void testEmptyMetadataFallsBackToConfig()
    {
        Map<String, String> metadata = Collections.emptyMap();
        assertThat(RestartLocalJobFactory.resolveRetryAttempts(metadata, config)).isEqualTo(3);
        assertThat(RestartLocalJobFactory.resolveHealthTimeout(metadata, config).toMillis())
        .isEqualTo(SecondBoundConfiguration.parse("60s").toMillis());
        assertThat(RestartLocalJobFactory.resolveNodeStateTransitionTimeout(metadata, config).toMillis())
        .isEqualTo(SecondBoundConfiguration.parse("600s").toMillis());
    }

    @Test
    void testMalformedOverrideFallsBackToConfig()
    {
        Map<String, String> metadata = new HashMap<>();
        metadata.put(RestartOperationMetadata.NODE_RESTART_RETRY_ATTEMPTS, "not-a-number");
        metadata.put(RestartOperationMetadata.CASSANDRA_HEALTH_TIMEOUT_SECONDS, "");

        assertThat(RestartLocalJobFactory.resolveRetryAttempts(metadata, config)).isEqualTo(3);
        assertThat(RestartLocalJobFactory.resolveHealthTimeout(metadata, config).toMillis())
        .isEqualTo(SecondBoundConfiguration.parse("60s").toMillis());
    }

    @Test
    void testWaitBetweenExecutionGroupsOverrideTakesPrecedence()
    {
        Map<String, String> metadata = new HashMap<>();
        metadata.put(RestartOperationMetadata.WAIT_BETWEEN_EXECUTION_GROUPS_SECONDS, "90");

        assertThat(newFactory().waitBetweenExecutionGroups(metadata).toSeconds()).isEqualTo(90);
    }

    @Test
    void testWaitBetweenExecutionGroupsFallsBackToConfig()
    {
        assertThat(newFactory().waitBetweenExecutionGroups(null).toSeconds())
        .isEqualTo(SecondBoundConfiguration.parse("45s").toSeconds());
        assertThat(newFactory().waitBetweenExecutionGroups(Collections.emptyMap()).toSeconds())
        .isEqualTo(SecondBoundConfiguration.parse("45s").toSeconds());
    }

    @Test
    void testMalformedWaitBetweenExecutionGroupsFallsBackToConfig()
    {
        Map<String, String> metadata = new HashMap<>();
        metadata.put(RestartOperationMetadata.WAIT_BETWEEN_EXECUTION_GROUPS_SECONDS, "not-a-number");

        assertThat(newFactory().waitBetweenExecutionGroups(metadata).toSeconds())
        .isEqualTo(SecondBoundConfiguration.parse("45s").toSeconds());
    }

    @Test
    void testZeroWaitBetweenExecutionGroupsOverrideDisablesTheWait()
    {
        Map<String, String> metadata = new HashMap<>();
        metadata.put(RestartOperationMetadata.WAIT_BETWEEN_EXECUTION_GROUPS_SECONDS, "0");

        assertThat(newFactory().waitBetweenExecutionGroups(metadata).toSeconds()).isZero();
    }

    @Test
    void testCreateJobForRestartWiresJobWithOperationIdentity()
    {
        RestartLocalJobFactory factory = newFactory();
        UUID operationId = UUID.randomUUID();
        UUID nodeId = UUID.randomUUID();
        String instanceHost = "127.0.0.1";

        LocalJob job = factory.createJob(operationId, nodeId, instanceHost, OperationType.RESTART);

        assertThat(job).isInstanceOf(RestartJob.class);
        assertThat(job.operationId()).isEqualTo(operationId);
        assertThat(job.nodeId()).isEqualTo(nodeId);
        assertThat(job.instanceHost()).isEqualTo(instanceHost);
        assertThat(job.operationType()).isEqualTo(OperationType.RESTART);
    }

    @Test
    void testCreateJobRejectsNonRestartOperationType()
    {
        RestartLocalJobFactory factory = newFactory();
        assertThatThrownBy(() ->
            factory.createJob(UUID.randomUUID(), UUID.randomUUID(), "127.0.0.1", OperationType.REPAIR))
        .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testNodeExecutionTimeoutBoundsAboveRestartWorstCase()
    {
        RestartLocalJobFactory factory = newFactory();
        // (health 60s + nodeRestart 600s x 2) x (retries 3 + 1) + 60s margin
        assertThat(factory.nodeExecutionTimeout().toSeconds()).isEqualTo((60 + 600 * 2) * 4 + 60);
    }
}
