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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.cassandra.sidecar.common.response.RingResponse;
import org.apache.cassandra.sidecar.common.response.data.RingEntry;

/**
 * Computes execution order for NetworkTopologyStrategy clusters. Under NTS, data is replicated across racks,
 * so only nodes within the same rack can be safely restarted in parallel. Racks are processed sequentially.
 */
public class NetworkTopologyExecutionOrderComputer implements NodeExecutionOrderComputer
{
    private final int maxRestartParallelism;

    /**
     * @param maxRestartParallelism the maximum number of nodes to restart in parallel within a single rack
     */
    public NetworkTopologyExecutionOrderComputer(int maxRestartParallelism)
    {
        this.maxRestartParallelism = maxRestartParallelism;
    }

    @Override
    public List<List<UUID>> computeOrder(RingResponse ring, List<UUID> nodes)
    {
        if (nodes.isEmpty())
        {
            return List.of();
        }

        Map<UUID, String> rackByNode = rackByNode(ring, nodes);

        // Group nodes by rack, using TreeMap for deterministic rack ordering
        Map<String, List<UUID>> nodesByRack = new TreeMap<>();
        for (UUID node : nodes)
        {
            nodesByRack.computeIfAbsent(rackByNode.get(node), k -> new ArrayList<>()).add(node);
        }

        // For each rack, create sequential groups of at most maxRestartParallelism nodes
        List<List<UUID>> executionOrder = new ArrayList<>();
        for (List<UUID> rackNodes : nodesByRack.values())
        {
            for (int i = 0; i < rackNodes.size(); i += maxRestartParallelism)
            {
                int end = Math.min(i + maxRestartParallelism, rackNodes.size());
                executionOrder.add(List.copyOf(rackNodes.subList(i, end)));
            }
        }
        return executionOrder;
    }

    private static Map<UUID, String> rackByNode(RingResponse ring, List<UUID> nodes)
    {
        Map<String, String> rackByHostId = new HashMap<>();
        for (RingEntry entry : ring)
        {
            if (entry.hostId() != null)
            {
                rackByHostId.put(entry.hostId(), entry.rack());
            }
        }

        Map<UUID, String> result = new HashMap<>();
        for (UUID node : nodes)
        {
            String rack = rackByHostId.get(node.toString());
            if (rack == null)
            {
                throw new IllegalArgumentException("Node " + node + " not found in ring");
            }
            result.put(node, rack);
        }
        return result;
    }
}
