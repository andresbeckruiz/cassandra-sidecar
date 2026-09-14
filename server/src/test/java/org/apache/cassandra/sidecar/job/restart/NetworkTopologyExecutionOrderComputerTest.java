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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.common.response.RingResponse;
import org.apache.cassandra.sidecar.common.response.data.RingEntry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link NetworkTopologyExecutionOrderComputer}
 */
class NetworkTopologyExecutionOrderComputerTest
{
    @Test
    void testSingleNode()
    {
        NetworkTopologyExecutionOrderComputer computer = new NetworkTopologyExecutionOrderComputer(1);
        UUID node = UUID.randomUUID();
        List<List<UUID>> result = computer.computeOrder(ringFrom(Map.of(node, "rack1")), List.of(node));
        assertThat(result).hasSize(1);
        assertThat(result.get(0)).containsExactly(node);
    }

    @Test
    void testSingleRackParallelismOne()
    {
        NetworkTopologyExecutionOrderComputer computer = new NetworkTopologyExecutionOrderComputer(1);
        UUID n1 = UUID.randomUUID();
        UUID n2 = UUID.randomUUID();
        UUID n3 = UUID.randomUUID();
        Map<UUID, String> rackByNode = Map.of(n1, "rack1", n2, "rack1", n3, "rack1");
        List<List<UUID>> result = computer.computeOrder(ringFrom(rackByNode), List.of(n1, n2, n3));
        assertThat(result).hasSize(3);
        for (List<UUID> group : result)
        {
            assertThat(group).hasSize(1);
        }
        assertThat(flattenAll(result)).containsExactlyInAnyOrder(n1, n2, n3);
    }

    @Test
    void testSingleRackParallelismTwo()
    {
        NetworkTopologyExecutionOrderComputer computer = new NetworkTopologyExecutionOrderComputer(2);
        UUID n1 = UUID.randomUUID();
        UUID n2 = UUID.randomUUID();
        UUID n3 = UUID.randomUUID();
        UUID n4 = UUID.randomUUID();
        Map<UUID, String> rackByNode = Map.of(n1, "rack1", n2, "rack1", n3, "rack1", n4, "rack1");
        List<List<UUID>> result = computer.computeOrder(ringFrom(rackByNode), List.of(n1, n2, n3, n4));
        assertThat(result).hasSize(2);
        for (List<UUID> group : result)
        {
            assertThat(group).hasSize(2);
        }
        assertThat(flattenAll(result)).containsExactlyInAnyOrder(n1, n2, n3, n4);
    }

    @Test
    void testMultipleRacksParallelismOne()
    {
        NetworkTopologyExecutionOrderComputer computer = new NetworkTopologyExecutionOrderComputer(1);
        UUID a1 = UUID.randomUUID();
        UUID a2 = UUID.randomUUID();
        UUID b1 = UUID.randomUUID();
        UUID b2 = UUID.randomUUID();
        UUID c1 = UUID.randomUUID();
        UUID c2 = UUID.randomUUID();
        Map<UUID, String> rackByNode = Map.of(a1, "rackA", a2, "rackA",
                                              b1, "rackB", b2, "rackB",
                                              c1, "rackC", c2, "rackC");
        List<List<UUID>> result = computer.computeOrder(ringFrom(rackByNode), List.of(a1, a2, b1, b2, c1, c2));
        // 3 racks x 2 nodes each, parallelism=1 → 6 groups (rack by rack)
        assertThat(result).hasSize(6);
        for (List<UUID> group : result)
        {
            assertThat(group).hasSize(1);
        }
        assertThat(flattenAll(result)).containsExactlyInAnyOrder(a1, a2, b1, b2, c1, c2);
    }

    @Test
    void testMultipleRacksParallelismTwo()
    {
        NetworkTopologyExecutionOrderComputer computer = new NetworkTopologyExecutionOrderComputer(2);
        UUID a1 = UUID.randomUUID();
        UUID a2 = UUID.randomUUID();
        UUID a3 = UUID.randomUUID();
        UUID a4 = UUID.randomUUID();
        UUID b1 = UUID.randomUUID();
        UUID b2 = UUID.randomUUID();
        UUID b3 = UUID.randomUUID();
        UUID b4 = UUID.randomUUID();
        UUID c1 = UUID.randomUUID();
        UUID c2 = UUID.randomUUID();
        UUID c3 = UUID.randomUUID();
        UUID c4 = UUID.randomUUID();
        Map<UUID, String> rackByNode = new HashMap<>();
        rackByNode.put(a1, "rackA"); rackByNode.put(a2, "rackA");
        rackByNode.put(a3, "rackA"); rackByNode.put(a4, "rackA");
        rackByNode.put(b1, "rackB"); rackByNode.put(b2, "rackB");
        rackByNode.put(b3, "rackB"); rackByNode.put(b4, "rackB");
        rackByNode.put(c1, "rackC"); rackByNode.put(c2, "rackC");
        rackByNode.put(c3, "rackC"); rackByNode.put(c4, "rackC");
        List<UUID> nodes = List.of(a1, a2, a3, a4, b1, b2, b3, b4, c1, c2, c3, c4);
        List<List<UUID>> result = computer.computeOrder(ringFrom(rackByNode), nodes);
        // 3 racks x 4 nodes each, parallelism=2 → 6 groups (2 per rack)
        assertThat(result).hasSize(6);
        for (List<UUID> group : result)
        {
            assertThat(group).hasSize(2);
        }
        assertThat(flattenAll(result)).containsExactlyInAnyOrder(nodes.toArray(new UUID[0]));
    }

    @Test
    void testRackParallelismExceedsNodesInRack()
    {
        NetworkTopologyExecutionOrderComputer computer = new NetworkTopologyExecutionOrderComputer(5);
        UUID n1 = UUID.randomUUID();
        UUID n2 = UUID.randomUUID();
        Map<UUID, String> rackByNode = Map.of(n1, "rack1", n2, "rack1");
        List<List<UUID>> result = computer.computeOrder(ringFrom(rackByNode), List.of(n1, n2));
        assertThat(result).hasSize(1);
        assertThat(result.get(0)).containsExactlyInAnyOrder(n1, n2);
    }

    @Test
    void testEmptyNodeList()
    {
        NetworkTopologyExecutionOrderComputer computer = new NetworkTopologyExecutionOrderComputer(1);
        List<List<UUID>> result = computer.computeOrder(new RingResponse(), List.of());
        assertThat(result).isEmpty();
    }

    @Test
    void testNodeNotInRingThrows()
    {
        NetworkTopologyExecutionOrderComputer computer = new NetworkTopologyExecutionOrderComputer(1);
        UUID node = UUID.randomUUID();
        assertThatThrownBy(() -> computer.computeOrder(new RingResponse(), List.of(node)))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testDeterministicOrdering()
    {
        NetworkTopologyExecutionOrderComputer computer = new NetworkTopologyExecutionOrderComputer(1);
        UUID a1 = UUID.randomUUID();
        UUID b1 = UUID.randomUUID();
        UUID c1 = UUID.randomUUID();
        Map<UUID, String> rackByNode = Map.of(a1, "rackA", b1, "rackB", c1, "rackC");
        List<UUID> nodes = List.of(a1, b1, c1);
        RingResponse ring = ringFrom(rackByNode);
        List<List<UUID>> result1 = computer.computeOrder(ring, nodes);
        List<List<UUID>> result2 = computer.computeOrder(ring, nodes);
        assertThat(result1).isEqualTo(result2);
    }

    @Test
    void testNodesOnlyFromSameRackInGroup()
    {
        NetworkTopologyExecutionOrderComputer computer = new NetworkTopologyExecutionOrderComputer(2);
        UUID a1 = UUID.randomUUID();
        UUID a2 = UUID.randomUUID();
        UUID b1 = UUID.randomUUID();
        UUID b2 = UUID.randomUUID();
        Map<UUID, String> rackByNode = Map.of(a1, "rackA", a2, "rackA", b1, "rackB", b2, "rackB");
        List<List<UUID>> result = computer.computeOrder(ringFrom(rackByNode), List.of(a1, a2, b1, b2));
        for (List<UUID> group : result)
        {
            Set<String> racksInGroup = new HashSet<>();
            for (UUID node : group)
            {
                racksInGroup.add(rackByNode.get(node));
            }
            assertThat(racksInGroup).hasSize(1);
        }
    }

    private List<UUID> flattenAll(List<List<UUID>> groups)
    {
        List<UUID> flat = new ArrayList<>();
        for (List<UUID> group : groups)
        {
            flat.addAll(group);
        }
        return flat;
    }

    private RingResponse ringFrom(Map<UUID, String> rackByNode)
    {
        RingResponse ring = new RingResponse();
        int i = 0;
        for (Map.Entry<UUID, String> entry : rackByNode.entrySet())
        {
            i++;
            ring.add(new RingEntry.Builder()
                     .datacenter("dc1").rack(entry.getValue()).status("Up")
                     .address("127.0.0." + i).port(9042).token(String.valueOf(i * 1000L))
                     .state("Normal").load("100 KiB").owns("").fqdn("localhost")
                     .hostId(entry.getKey().toString()).build());
        }
        return ring;
    }
}
