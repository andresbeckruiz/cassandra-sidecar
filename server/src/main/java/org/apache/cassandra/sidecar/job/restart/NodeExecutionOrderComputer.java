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

import java.util.List;
import java.util.UUID;

import org.apache.cassandra.sidecar.common.response.RingResponse;

/**
 * Computes the execution order for a rolling restart. The result is a list of sequential groups
 * where nodes within each group can be restarted in parallel.
 */
public interface NodeExecutionOrderComputer
{
    /**
     * Computes the execution order for the given nodes using the cluster topology. Implementations derive
     * whatever placement information they need (e.g. racks) from the ring.
     *
     * @param ring  the current ring state, describing cluster topology
     * @param nodes the nodes to restart
     * @return sequential groups of nodes; nodes within each group can be restarted in parallel
     */
    List<List<UUID>> computeOrder(RingResponse ring, List<UUID> nodes);
}
