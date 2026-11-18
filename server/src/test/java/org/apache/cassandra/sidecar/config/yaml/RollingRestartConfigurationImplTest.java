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

import java.io.IOException;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.RollingRestartConfiguration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link RollingRestartConfigurationImpl}
 */
class RollingRestartConfigurationImplTest
{
    @Test
    void testDefaults()
    {
        assertDefaults(new RollingRestartConfigurationImpl());
    }

    @Test
    void testEmptyYamlValuesUseDefaults() throws IOException
    {
        String yaml = "operational_job:\n"
                      + "  rolling_restart:\n"
                      + "    cassandra_health_timeout:\n"
                      + "    node_state_transition_timeout:\n"
                      + "    node_restart_retry_attempts:\n"
                      + "    wait_between_execution_groups:\n";

        RollingRestartConfiguration config = SidecarConfigurationImpl.fromYamlString(yaml)
                                                                     .operationalJobConfiguration()
                                                                     .rollingRestartConfiguration();

        assertDefaults(config);
    }

    @Test
    void testCustomValues()
    {
        RollingRestartConfigurationImpl config = RollingRestartConfigurationImpl.builder()
                                                                               .enabled(true)
                                                                               .cassandraHealthTimeout(SecondBoundConfiguration.parse("120s"))
                                                                               .nodeStateTransitionTimeout(SecondBoundConfiguration.parse("300s"))
                                                                               .nodeRestartRetryAttempts(5)
                                                                               .waitBetweenExecutionGroups(SecondBoundConfiguration.parse("90s"))
                                                                               .build();
        assertThat(config.enabled()).isTrue();
        assertThat(config.cassandraHealthTimeout()).isEqualTo(SecondBoundConfiguration.parse("120s"));
        assertThat(config.nodeStateTransitionTimeout()).isEqualTo(SecondBoundConfiguration.parse("300s"));
        assertThat(config.nodeRestartRetryAttempts()).isEqualTo(5);
        assertThat(config.waitBetweenExecutionGroups()).isEqualTo(SecondBoundConfiguration.parse("90s"));
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

    private static void assertDefaults(RollingRestartConfiguration config)
    {
        assertThat(config.enabled()).isFalse();
        assertThat(config.cassandraHealthTimeout()).isEqualTo(SecondBoundConfiguration.parse("60s"));
        assertThat(config.nodeStateTransitionTimeout()).isEqualTo(SecondBoundConfiguration.parse("600s"));
        assertThat(config.nodeRestartRetryAttempts()).isEqualTo(3);
        assertThat(config.waitBetweenExecutionGroups()).isEqualTo(SecondBoundConfiguration.parse("3m"));
    }
}
