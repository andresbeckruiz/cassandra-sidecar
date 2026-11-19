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
import static org.mockito.Mockito.verifyNoInteractions;

/**
 * Tests for {@link RestartLocalJobFactory}
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
    private final RestartLocalJobFactory factory = new RestartLocalJobFactory(metadataFetcher,
                                                                              mock(LifecycleManager.class),
                                                                              mock(RingHealthChecker.class),
                                                                              config);

    @Test
    void testCreateJobForRestartWiresJobWithOperationIdentityWithoutReadingTheRing()
    {
        UUID operationId = UUID.randomUUID();
        UUID nodeId = UUID.randomUUID();

        LocalJob job = factory.createJob(operationId, nodeId, "127.0.0.1", OperationType.RESTART);

        assertThat(job).isInstanceOf(RestartJob.class);
        assertThat(job.operationId()).isEqualTo(operationId);
        assertThat(job.nodeId()).isEqualTo(nodeId);
        assertThat(job.instanceHost()).isEqualTo("127.0.0.1");
        assertThat(job.operationType()).isEqualTo(OperationType.RESTART);
        verifyNoInteractions(metadataFetcher);
    }

    @Test
    void testCreateJobRejectsNonRestartOperationType()
    {
        assertThatThrownBy(() -> factory.createJob(UUID.randomUUID(), UUID.randomUUID(), "127.0.0.1",
                                                   OperationType.REPAIR))
        .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testRestartRequiresPredecessorSuccessAndCanRecover()
    {
        assertThat(factory.requiresPredecessorSuccess()).isTrue();
        assertThat(factory.canRecover()).isTrue();
    }

    @Test
    void testNodeExecutionTimeoutBoundsAboveRestartWorstCase()
    {
        // (health check 60s + state transition 600s x 2) x (retries 3 + 1) + 60s margin
        assertThat(factory.nodeExecutionTimeout().toSeconds()).isEqualTo((60 + 600 * 2) * 4 + 60);
    }

}
