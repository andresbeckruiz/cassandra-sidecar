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
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link RollingRestartJob}
 */
class RollingRestartJobTest
{
    @Test
    void testHasConflictWithExistingRestartSameDatacenter()
    {
        RollingRestartJob job = restartJob("dc1");
        RollingRestartJob existingJob = restartJob("dc1");
        assertThat(job.hasConflict(List.of(existingJob))).isTrue();
    }

    @Test
    void testHasConflictDifferentDatacenter()
    {
        // The active-operation lock is datacenter-scoped, so a restart in another datacenter is not a conflict.
        RollingRestartJob job = restartJob("dc1");
        RollingRestartJob otherDc = restartJob("dc2");
        assertThat(job.hasConflict(List.of(otherDc))).isFalse();
    }

    @Test
    void testHasConflictNoneExisting()
    {
        RollingRestartJob job = restartJob("dc1");
        assertThat(job.hasConflict(Collections.emptyList())).isFalse();
    }

    @Test
    void testStatusRemainsRunningAfterHandoffCompletes()
    {
        RollingRestartJob job = restartJob("dc1");
        Promise<Void> promise = Promise.promise();
        job.execute(promise);
        // The handoff future completes, but the coordinated restart is only just starting: status must stay
        // RUNNING (not derive SUCCEEDED from the completed future) so POST /restart reports 202/RUNNING.
        assertThat(promise.future().succeeded()).isTrue();
        assertThat(job.status()).isEqualTo(OperationalJobStatus.RUNNING);
    }

    @Test
    void testStatusIsFailedWhenJobFailsToStart()
    {
        RollingRestartJob job = restartJob("dc1");
        job.failToStart(new RuntimeException("An active operation already exists"));
        assertThat(job.status()).isEqualTo(OperationalJobStatus.FAILED);
    }

    private RollingRestartJob restartJob(String targetDatacenter)
    {
        return new RollingRestartJob(UUIDs.timeBased(),
                                     List.of(List.of(UUID.randomUUID())),
                                     Map.of(),
                                     targetDatacenter);
    }
}
