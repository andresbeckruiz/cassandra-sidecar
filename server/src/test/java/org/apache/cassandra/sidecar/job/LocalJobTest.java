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

import java.util.UUID;

import org.junit.jupiter.api.Test;

import io.vertx.core.Future;
import org.apache.cassandra.sidecar.common.data.OperationType;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link LocalJob}
 */
class LocalJobTest
{
    private static final UUID OPERATION_ID = UUID.randomUUID();
    private static final UUID NODE_ID = UUID.randomUUID();
    private static final String INSTANCE_HOST = "127.0.0.1";
    private static final OperationType OPERATION_TYPE = OperationType.DRAIN;

    @Test
    void testStatusTransitionsOnSuccess()
    {
        LocalJob job = createSucceedingJob();
        assertThat(job.status()).isEqualTo(OperationalJobStatus.CREATED);

        job.execute();
        assertThat(job.status()).isEqualTo(OperationalJobStatus.SUCCEEDED);
    }

    @Test
    void testStatusTransitionsOnFailure()
    {
        LocalJob job = createFailingJob();
        assertThat(job.status()).isEqualTo(OperationalJobStatus.CREATED);

        assertThatThrownBy(job::execute).isInstanceOf(RuntimeException.class);
        assertThat(job.status()).isEqualTo(OperationalJobStatus.FAILED);
    }

    @Test
    void testOnJobFailedDefaultIsNoOp()
    {
        LocalJob job = createSucceedingJob();
        Future<Void> result = job.onJobFailed();
        assertThat(result.succeeded()).isTrue();
    }

    @Test
    void testFieldAccessors()
    {
        LocalJob job = createSucceedingJob();
        assertThat(job.operationId()).isEqualTo(OPERATION_ID);
        assertThat(job.nodeId()).isEqualTo(NODE_ID);
        assertThat(job.instanceHost()).isEqualTo(INSTANCE_HOST);
        assertThat(job.operationType()).isEqualTo(OPERATION_TYPE);
    }

    private static LocalJob createSucceedingJob()
    {
        return new LocalJob(OPERATION_ID, NODE_ID, INSTANCE_HOST, OPERATION_TYPE)
        {
            @Override
            protected void executeInternal()
            {
            }
        };
    }

    private static LocalJob createFailingJob()
    {
        return new LocalJob(OPERATION_ID, NODE_ID, INSTANCE_HOST, OPERATION_TYPE)
        {
            @Override
            protected void executeInternal()
            {
                throw new RuntimeException("test failure");
            }
        };
    }
}
