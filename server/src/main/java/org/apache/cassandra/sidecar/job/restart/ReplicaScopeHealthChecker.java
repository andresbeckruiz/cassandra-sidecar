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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import org.apache.cassandra.sidecar.common.response.RingResponse;
import org.apache.cassandra.sidecar.common.response.data.RingEntry;

/**
 * Health checker that consults data replication scope across a cluster. 
 *
 * <p>The set of potential co-replicas depends on the datacenter's replication strategy (see
 * {@link ReplicaScope}). Under the common case ({@link ReplicaScope#RACK} — NetworkTopologyStrategy
 * with RF less than the number of racks) only nodes in other racks of the same datacenter can be co-replicas, so
 * same-rack and other-datacenter nodes are ignored. When that precondition does not hold, the checker widens the
 * set of nodes it requires to be healthy. </p>
 */
public class ReplicaScopeHealthChecker implements RestartHealthChecker
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicaScopeHealthChecker.class);

    private final ReplicationStrategyAnalyzer strategyAnalyzer;

    @Inject
    public ReplicaScopeHealthChecker(ReplicationStrategyAnalyzer strategyAnalyzer)
    {
        this.strategyAnalyzer = strategyAnalyzer;
    }

    @Override
    public boolean isHealthy(RingResponse ring, UUID localNodeId)
    {
        RingEntry local = findEntry(ring, localNodeId);
        if (local == null)
        {
            LOGGER.warn("Node {} not found in ring; cannot verify it is safe to take down", localNodeId);
            return false;
        }

        String localRack = local.rack();
        String localDatacenter = local.datacenter();
        ReplicaScope scope = strategyAnalyzer.replicaScopeForDatacenter(ring, localDatacenter);

        String localId = localNodeId.toString();
        for (RingEntry entry : ring)
        {
            if (localId.equals(entry.hostId()))
            {
                continue;
            }
            if (!isPotentialCoReplica(scope, localDatacenter, localRack, entry))
            {
                continue;
            }
            // Only safe when co-replica is confirmed up and in the normal state. A "?" status means
            // liveness is unknown, and a non-normal state (Joining/Leaving/Moving) means data is being
            // redistributed, so in either case taking a node down could remove a replica that is still needed.
            if (!"Up".equalsIgnoreCase(entry.status()) || !"Normal".equalsIgnoreCase(entry.state()))
            {
                LOGGER.debug("Node {} in datacenter {} rack {} is not confirmed up and normal (status={}, state={}); "
                             + "taking a node down is unsafe under {} scope", entry.hostId(), entry.datacenter(),
                             entry.rack(), entry.status(), entry.state(), scope);
                return false;
            }
        }
        return true;
    }

    /**
     * Whether {@code entry} could hold a replica alongside the local node, given the datacenter's replication scope
     * across all keyspaces.
     */
    private static boolean isPotentialCoReplica(ReplicaScope scope, String localDatacenter, String localRack,
                                                RingEntry entry)
    {
        switch (scope)
        {
            case RACK:
                // Only nodes in other racks of the same datacenter can be co-replicas.
                return localDatacenter.equals(entry.datacenter()) && !localRack.equals(entry.rack());
            case DATACENTER:
                // A rack may hold multiple replicas, so any node in the datacenter can be a co-replica.
                return localDatacenter.equals(entry.datacenter());
            case CLUSTER:
                // Replicas can be placed anywhere in the cluster.
                return true;
            default:
                throw new IllegalArgumentException("Unknown ReplicaScope: " + scope);
        }
    }

    private static RingEntry findEntry(RingResponse ring, UUID nodeId)
    {
        String id = nodeId.toString();
        for (RingEntry entry : ring)
        {
            if (id.equals(entry.hostId()))
            {
                return entry;
            }
        }
        return null;
    }
}
