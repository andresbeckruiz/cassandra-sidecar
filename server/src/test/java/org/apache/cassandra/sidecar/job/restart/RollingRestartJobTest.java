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
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import com.datastax.driver.core.utils.UUIDs;
import io.vertx.core.Promise;
import org.apache.cassandra.sidecar.common.data.OperationType;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link RollingRestartJob}
 */
class RollingRestartJobTest
{
    @Test
    void testIsCoordinatedAndLeavesLockReleaseToTheCoordinator()
    {
        List<List<UUID>> executionOrder = List.of(List.of(UUID.randomUUID()), List.of(UUID.randomUUID()));
        RollingRestartJob job = new RollingRestartJob(UUIDs.timeBased(), executionOrder, Map.of());

        assertThat(job.operationType()).isEqualTo(OperationType.RESTART);
        assertThat(job.requiresCoordination()).isTrue();
        assertThat(job.releasesOnCompletion()).isFalse();
        assertThat(job.nodeExecutionOrder()).isEqualTo(executionOrder);
    }

    @Test
    void testHasConflictOnlyWithInflightRestart()
    {
        assertThat(restartJob().hasConflict(List.of(restartJob()))).isTrue();
        assertThat(restartJob().hasConflict(Collections.emptyList())).isFalse();
    }

    @Test
    void testStatusRemainsRunningAfterHandoffCompletes()
    {
        RollingRestartJob job = restartJob();
        Promise<Void> promise = Promise.promise();

        job.execute(promise);

        assertThat(promise.future().succeeded()).isTrue();
        assertThat(job.status()).isEqualTo(OperationalJobStatus.RUNNING);
    }

    @Test
    void testStatusIsFailedWhenJobFailsToStart()
    {
        RollingRestartJob job = restartJob();

        job.failToStart(new RuntimeException("An active operation already exists"));

        assertThat(job.status()).isEqualTo(OperationalJobStatus.FAILED);
    }

    private static RollingRestartJob restartJob()
    {
        return new RollingRestartJob(UUIDs.timeBased(), List.of(List.of(UUID.randomUUID())), Map.of());
    }
}
