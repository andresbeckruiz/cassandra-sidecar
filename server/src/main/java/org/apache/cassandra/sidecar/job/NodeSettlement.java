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

import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.jetbrains.annotations.Nullable;

/**
 * How a node row in storage is settled when its operation stops before the node reports an outcome of its own.
 *
 * <p>A Sidecar settles the rows of its own instances, and an operator forcing an abort settles the rows of
 * Sidecars that cannot settle their own. Both classify a node the same way, so the rule is defined here.</p>
 */
public enum NodeSettlement
{
    /**
     * The node reached a terminal status already, so nothing is written for it.
     */
    ALREADY_SETTLED,

    /**
     * The operation stopped before the node started, so the node was never touched.
     */
    NOT_STARTED,

    /**
     * The node was executing when the operation stopped, so the work it started has an unknown outcome.
     */
    ABANDONED_MID_EXECUTION;

    /**
     * Classifies a node by the status recorded for it.
     *
     * @param nodeStatus the status recorded for the node, or {@code null} if the node has no row yet
     * @return the settlement that applies to the node
     */
    public static NodeSettlement of(@Nullable OperationalJobStatus nodeStatus)
    {
        if (nodeStatus == null)
        {
            return NOT_STARTED;
        }
        if (nodeStatus.isCompleted())
        {
            return ALREADY_SETTLED;
        }
        return nodeStatus == OperationalJobStatus.RUNNING ? ABANDONED_MID_EXECUTION : NOT_STARTED;
    }

    /**
     * The terminal status to write to the node row.
     *
     * <p>A node that never started records the operation's own terminal status, so an aborted operation leaves
     * {@link OperationalJobStatus#ABORTED} on the nodes it never reached. A node abandoned mid-execution records
     * {@link OperationalJobStatus#FAILED}, because the operation stopped without learning whether the work
     * finished.</p>
     *
     * @param operationStatus the terminal status of the operation
     * @return the status to write to the node row
     * @throws IllegalStateException if this is {@link #ALREADY_SETTLED}, which keeps the status it reported
     */
    public OperationalJobStatus terminalStatus(OperationalJobStatus operationStatus)
    {
        switch (this)
        {
            case NOT_STARTED:
                return operationStatus;
            case ABANDONED_MID_EXECUTION:
                return OperationalJobStatus.FAILED;
            default:
                throw new IllegalStateException("A node that has settled already keeps the status it reported");
        }
    }
}
