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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link NodeSettlement}
 */
class NodeSettlementTest
{
    @Test
    void testNodeWithNoRowNeverStarted()
    {
        assertThat(NodeSettlement.of(null)).isEqualTo(NodeSettlement.NOT_STARTED);
    }

    @Test
    void testCreatedNodeNeverStarted()
    {
        assertThat(NodeSettlement.of(OperationalJobStatus.CREATED)).isEqualTo(NodeSettlement.NOT_STARTED);
    }

    @Test
    void testRunningNodeAbandonedMidExecution()
    {
        assertThat(NodeSettlement.of(OperationalJobStatus.RUNNING))
        .isEqualTo(NodeSettlement.ABANDONED_MID_EXECUTION);
    }

    @ParameterizedTest
    @EnumSource(value = OperationalJobStatus.class, names = { "SUCCEEDED", "FAILED", "ABORTED" })
    void testTerminalNodeAlreadySettled(OperationalJobStatus status)
    {
        assertThat(NodeSettlement.of(status)).isEqualTo(NodeSettlement.ALREADY_SETTLED);
    }

    @ParameterizedTest
    @EnumSource(value = OperationalJobStatus.class, names = { "FAILED", "ABORTED" })
    void testNodeThatNeverStartedRecordsTheOperationStatus(OperationalJobStatus operationStatus)
    {
        assertThat(NodeSettlement.NOT_STARTED.terminalStatus(operationStatus)).isEqualTo(operationStatus);
    }

    @ParameterizedTest
    @EnumSource(value = OperationalJobStatus.class, names = { "FAILED", "ABORTED" })
    void testNodeAbandonedMidExecutionRecordsFailed(OperationalJobStatus operationStatus)
    {
        assertThat(NodeSettlement.ABANDONED_MID_EXECUTION.terminalStatus(operationStatus))
        .isEqualTo(OperationalJobStatus.FAILED);
    }

}
