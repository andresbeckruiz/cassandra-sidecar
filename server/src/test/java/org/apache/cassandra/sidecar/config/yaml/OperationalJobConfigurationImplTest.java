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

package org.apache.cassandra.sidecar.config.yaml;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.RollingRestartConfiguration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link OperationalJobConfigurationImpl}
 */
class OperationalJobConfigurationImplTest
{
    @Test
    void testDefaults()
    {
        OperationalJobConfigurationImpl config = new OperationalJobConfigurationImpl();
        assertThat(config.tablesTtl()).isEqualTo(SecondBoundConfiguration.parse("90d"));
        assertThat(config.coordinationEnabled()).isFalse();
        assertThat(config.localJobCoordinationDelay()).isEqualTo(SecondBoundConfiguration.parse("3m"));
        assertThat(config.nodeExecutionTimeout()).isEqualTo(SecondBoundConfiguration.parse("10m"));
        assertThat(config.durableTrackingEnabled()).isFalse();
        assertThat(config.rollingRestartConfiguration()).isNotNull();
    }

    @Test
    void testTtlBelowMinimumThrows()
    {
        assertThatThrownBy(() -> OperationalJobConfigurationImpl.builder()
                                                                .tablesTtl(SecondBoundConfiguration.parse("13d"))
                                                                .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("tablesTtl cannot be less than");
    }

    @Test
    void testCustomValues()
    {
        OperationalJobConfigurationImpl config = OperationalJobConfigurationImpl.builder()
                                                                                .tablesTtl(SecondBoundConfiguration.parse("120d"))
                                                                                .coordinationEnabled(false)
                                                                                .localJobCoordinationDelay(SecondBoundConfiguration.parse("1m"))
                                                                                .nodeExecutionTimeout(SecondBoundConfiguration.parse("30m"))
                                                                                .durableTrackingEnabled(true)
                                                                                .build();
        assertThat(config.tablesTtl()).isEqualTo(SecondBoundConfiguration.parse("120d"));
        assertThat(config.coordinationEnabled()).isFalse();
        assertThat(config.localJobCoordinationDelay()).isEqualTo(SecondBoundConfiguration.parse("1m"));
        assertThat(config.nodeExecutionTimeout()).isEqualTo(SecondBoundConfiguration.parse("30m"));
        assertThat(config.durableTrackingEnabled()).isTrue();
    }

    @Test
    void testRollingRestartDefaults()
    {
        OperationalJobConfigurationImpl config = new OperationalJobConfigurationImpl();
        RollingRestartConfiguration restartConfig = config.rollingRestartConfiguration();
        assertThat(restartConfig.enabled()).isFalse();
        assertThat(restartConfig.cassandraHealthTimeout()).isEqualTo(SecondBoundConfiguration.parse("60s"));
        assertThat(restartConfig.nodeStateTransitionTimeout()).isEqualTo(SecondBoundConfiguration.parse("600s"));
        assertThat(restartConfig.nodeRestartRetryAttempts()).isEqualTo(3);
        assertThat(restartConfig.waitBetweenExecutionGroups()).isEqualTo(SecondBoundConfiguration.parse("3m"));
    }

    @Test
    void testRollingRestartCustomValues()
    {
        RollingRestartConfigurationImpl restartConfig = RollingRestartConfigurationImpl.builder()
                                                                                      .enabled(true)
                                                                                      .cassandraHealthTimeout(SecondBoundConfiguration.parse("120s"))
                                                                                      .nodeStateTransitionTimeout(SecondBoundConfiguration.parse("300s"))
                                                                                      .nodeRestartRetryAttempts(5)
                                                                                      .waitBetweenExecutionGroups(SecondBoundConfiguration.parse("90s"))
                                                                                      .build();
        OperationalJobConfigurationImpl config = OperationalJobConfigurationImpl.builder()
                                                                                .rollingRestartConfiguration(restartConfig)
                                                                                .build();
        RollingRestartConfiguration result = config.rollingRestartConfiguration();
        assertThat(result.enabled()).isTrue();
        assertThat(result.cassandraHealthTimeout()).isEqualTo(SecondBoundConfiguration.parse("120s"));
        assertThat(result.nodeStateTransitionTimeout()).isEqualTo(SecondBoundConfiguration.parse("300s"));
        assertThat(result.nodeRestartRetryAttempts()).isEqualTo(5);
        assertThat(result.waitBetweenExecutionGroups()).isEqualTo(SecondBoundConfiguration.parse("90s"));
    }

    @Test
    void testZeroWaitBetweenExecutionGroupsDisablesTheWait()
    {
        RollingRestartConfigurationImpl config = RollingRestartConfigurationImpl.builder()
                                                                               .waitBetweenExecutionGroups(SecondBoundConfiguration.parse("0s"))
                                                                               .build();
        assertThat(config.waitBetweenExecutionGroups().toSeconds()).isZero();
    }

    @Test
    void testNegativeRetryAttemptsThrows()
    {
        assertThatThrownBy(() -> RollingRestartConfigurationImpl.builder()
                                                               .nodeRestartRetryAttempts(-1)
                                                               .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("node_restart_retry_attempts must not be negative");
    }

    @Test
    void testZeroCassandraHealthTimeoutThrows()
    {
        assertThatThrownBy(() -> RollingRestartConfigurationImpl.builder()
                                                               .cassandraHealthTimeout(SecondBoundConfiguration.parse("0s"))
                                                               .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cassandra_health_timeout must be greater than 0");
    }

    @Test
    void testZeroNodeStateTransitionTimeoutThrows()
    {
        assertThatThrownBy(() -> RollingRestartConfigurationImpl.builder()
                                                               .nodeStateTransitionTimeout(SecondBoundConfiguration.parse("0s"))
                                                               .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("node_state_transition_timeout must be greater than 0");
    }
}
