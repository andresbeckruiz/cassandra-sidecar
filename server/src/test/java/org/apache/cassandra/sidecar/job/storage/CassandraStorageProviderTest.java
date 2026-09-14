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

package org.apache.cassandra.sidecar.job.storage;

import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.datastax.driver.core.utils.UUIDs;
import org.apache.cassandra.sidecar.common.data.OperationType;
import org.apache.cassandra.sidecar.common.response.NodeSettings;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.config.DriverConfiguration;
import org.apache.cassandra.sidecar.db.ActiveClusterOpsDatabaseAccessor;
import org.apache.cassandra.sidecar.db.ClusterOpsDatabaseAccessor;
import org.apache.cassandra.sidecar.db.ClusterOpsNodeStateDatabaseAccessor;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link CassandraStorageProvider}, focused on local-datacenter resolution.
 */
class CassandraStorageProviderTest
{
    private static final String CLUSTER_NAME = "test-cluster";
    private static final UUID OPERATION_ID = UUIDs.timeBased();

    private ActiveClusterOpsDatabaseAccessor activeOpsAccessor;
    private InstanceMetadataFetcher instanceMetadataFetcher;
    private DriverConfiguration driverConfiguration;
    private CassandraStorageProvider provider;

    @BeforeEach
    void setup()
    {
        activeOpsAccessor = mock(ActiveClusterOpsDatabaseAccessor.class);
        instanceMetadataFetcher = mock(InstanceMetadataFetcher.class);
        driverConfiguration = mock(DriverConfiguration.class);
        // clusterName is supplied so initialize() only needs to resolve the datacenter (no CQL session use).
        provider = new CassandraStorageProvider(mock(CQLSessionProvider.class),
                                                mock(ClusterOpsDatabaseAccessor.class),
                                                mock(ClusterOpsNodeStateDatabaseAccessor.class),
                                                activeOpsAccessor,
                                                instanceMetadataFetcher,
                                                driverConfiguration,
                                                CLUSTER_NAME);
    }

    @Test
    void testResolvesDatacenterFromNodeSettings()
    {
        when(instanceMetadataFetcher.<NodeSettings>callOnFirstAvailableInstance(any()))
        .thenReturn(NodeSettings.builder().datacenter("dc-from-node").build());

        provider.trySetActiveOperation(OperationType.RESTART, OPERATION_ID, null);

        verify(activeOpsAccessor).trySetActiveOperation(eq(CLUSTER_NAME), eq("dc-from-node"),
                                                        eq(OperationType.RESTART), eq(OPERATION_ID));
    }

    @Test
    void testFallsBackToConfiguredLocalDcWhenNodeSettingsUnavailable()
    {
        // Cassandra 3.x has no node settings endpoint — resolution must fall back to the configured driver local_dc.
        when(instanceMetadataFetcher.<NodeSettings>callOnFirstAvailableInstance(any()))
        .thenThrow(new RuntimeException("node settings unavailable"));
        when(driverConfiguration.localDc()).thenReturn("dc-from-config");

        provider.trySetActiveOperation(OperationType.RESTART, OPERATION_ID, null);

        verify(activeOpsAccessor).trySetActiveOperation(eq(CLUSTER_NAME), eq("dc-from-config"),
                                                        eq(OperationType.RESTART), eq(OPERATION_ID));
    }

    @Test
    void testFallsBackToConfiguredLocalDcWhenNodeSettingsDatacenterMissing()
    {
        when(instanceMetadataFetcher.<NodeSettings>callOnFirstAvailableInstance(any()))
        .thenReturn(NodeSettings.builder().build());
        when(driverConfiguration.localDc()).thenReturn("dc-from-config");

        provider.trySetActiveOperation(OperationType.RESTART, OPERATION_ID, null);

        verify(activeOpsAccessor).trySetActiveOperation(eq(CLUSTER_NAME), eq("dc-from-config"),
                                                        eq(OperationType.RESTART), eq(OPERATION_ID));
    }

    @Test
    void testThrowsWhenNoDatacenterResolvable()
    {
        when(instanceMetadataFetcher.<NodeSettings>callOnFirstAvailableInstance(any()))
        .thenThrow(new RuntimeException("node settings unavailable"));
        when(driverConfiguration.localDc()).thenReturn(null);

        assertThatThrownBy(() -> provider.trySetActiveOperation(OperationType.RESTART, OPERATION_ID, null))
        .isInstanceOf(StorageProviderException.class)
        .hasMessageContaining("Failed to resolve local datacenter");
    }

    @Test
    void testExplicitTargetDatacenterOverridesResolvedLocalDatacenter()
    {
        when(instanceMetadataFetcher.<NodeSettings>callOnFirstAvailableInstance(any()))
        .thenReturn(NodeSettings.builder().datacenter("dc-from-node").build());

        provider.trySetActiveOperation(OperationType.RESTART, OPERATION_ID, "explicit-dc");

        verify(activeOpsAccessor).trySetActiveOperation(eq(CLUSTER_NAME), eq("explicit-dc"),
                                                        eq(OperationType.RESTART), eq(OPERATION_ID));
    }
}
