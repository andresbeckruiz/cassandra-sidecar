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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.common.DataObjectBuilder;
import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.RollingRestartConfiguration;

/**
 * Configuration for rolling restart operations
 */
public class RollingRestartConfigurationImpl implements RollingRestartConfiguration
{
    private static final boolean DEFAULT_ENABLED = false;
    private static final SecondBoundConfiguration DEFAULT_CASSANDRA_HEALTH_TIMEOUT = SecondBoundConfiguration.parse("60s");
    private static final SecondBoundConfiguration DEFAULT_NODE_STATE_TRANSITION_TIMEOUT = SecondBoundConfiguration.parse("600s");
    private static final int DEFAULT_NODE_RESTART_RETRY_ATTEMPTS = 3;
    private static final SecondBoundConfiguration DEFAULT_WAIT_BETWEEN_EXECUTION_GROUPS = SecondBoundConfiguration.parse("3m");

    protected boolean enabled;
    protected SecondBoundConfiguration cassandraHealthTimeout;
    protected SecondBoundConfiguration nodeStateTransitionTimeout;
    protected int nodeRestartRetryAttempts;
    protected SecondBoundConfiguration waitBetweenExecutionGroups;

    public RollingRestartConfigurationImpl()
    {
        this(builder());
    }

    protected RollingRestartConfigurationImpl(Builder builder)
    {
        this.enabled = builder.enabled;
        setCassandraHealthTimeout(builder.cassandraHealthTimeout);
        setNodeStateTransitionTimeout(builder.nodeStateTransitionTimeout);
        setNodeRestartRetryAttempts(builder.nodeRestartRetryAttempts);
        setWaitBetweenExecutionGroups(builder.waitBetweenExecutionGroups);
    }

    @Override
    @JsonProperty(value = "enabled")
    public boolean enabled()
    {
        return enabled;
    }

    @JsonProperty(value = "enabled")
    public void setEnabled(boolean enabled)
    {
        this.enabled = enabled;
    }

    @Override
    @JsonProperty(value = "cassandra_health_timeout")
    public SecondBoundConfiguration cassandraHealthTimeout()
    {
        return cassandraHealthTimeout;
    }

    @JsonProperty(value = "cassandra_health_timeout")
    public void setCassandraHealthTimeout(SecondBoundConfiguration cassandraHealthTimeout)
    {
        if (cassandraHealthTimeout != null && cassandraHealthTimeout.toSeconds() <= 0)
        {
            throw new IllegalArgumentException("cassandra_health_timeout must be greater than 0");
        }
        this.cassandraHealthTimeout = cassandraHealthTimeout;
    }

    @Override
    @JsonProperty(value = "node_state_transition_timeout")
    public SecondBoundConfiguration nodeStateTransitionTimeout()
    {
        return nodeStateTransitionTimeout;
    }

    @JsonProperty(value = "node_state_transition_timeout")
    public void setNodeStateTransitionTimeout(SecondBoundConfiguration nodeStateTransitionTimeout)
    {
        if (nodeStateTransitionTimeout != null && nodeStateTransitionTimeout.toSeconds() <= 0)
        {
            throw new IllegalArgumentException("node_state_transition_timeout must be greater than 0");
        }
        this.nodeStateTransitionTimeout = nodeStateTransitionTimeout;
    }

    @Override
    @JsonProperty(value = "node_restart_retry_attempts")
    public int nodeRestartRetryAttempts()
    {
        return nodeRestartRetryAttempts;
    }

    @JsonProperty(value = "node_restart_retry_attempts")
    public void setNodeRestartRetryAttempts(int nodeRestartRetryAttempts)
    {
        if (nodeRestartRetryAttempts < 0)
        {
            throw new IllegalArgumentException("node_restart_retry_attempts must not be negative");
        }
        this.nodeRestartRetryAttempts = nodeRestartRetryAttempts;
    }

    @Override
    @JsonProperty(value = "wait_between_execution_groups")
    public SecondBoundConfiguration waitBetweenExecutionGroups()
    {
        return waitBetweenExecutionGroups;
    }

    // Zero is a valid value meaning "no wait", and the duration parser already rejects negatives, so unlike the
    // timeouts above this setter needs no range guard.
    @JsonProperty(value = "wait_between_execution_groups")
    public void setWaitBetweenExecutionGroups(SecondBoundConfiguration waitBetweenExecutionGroups)
    {
        this.waitBetweenExecutionGroups = waitBetweenExecutionGroups;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * {@code RollingRestartConfigurationImpl} builder static inner class.
     */
    public static class Builder implements DataObjectBuilder<Builder, RollingRestartConfigurationImpl>
    {
        private boolean enabled = DEFAULT_ENABLED;
        private SecondBoundConfiguration cassandraHealthTimeout = DEFAULT_CASSANDRA_HEALTH_TIMEOUT;
        private SecondBoundConfiguration nodeStateTransitionTimeout = DEFAULT_NODE_STATE_TRANSITION_TIMEOUT;
        private int nodeRestartRetryAttempts = DEFAULT_NODE_RESTART_RETRY_ATTEMPTS;
        private SecondBoundConfiguration waitBetweenExecutionGroups = DEFAULT_WAIT_BETWEEN_EXECUTION_GROUPS;

        protected Builder()
        {
        }

        @Override
        public Builder self()
        {
            return this;
        }

        public Builder enabled(boolean enabled)
        {
            return update(b -> b.enabled = enabled);
        }

        public Builder cassandraHealthTimeout(SecondBoundConfiguration cassandraHealthTimeout)
        {
            return update(b -> b.cassandraHealthTimeout = cassandraHealthTimeout);
        }

        public Builder nodeStateTransitionTimeout(SecondBoundConfiguration nodeStateTransitionTimeout)
        {
            return update(b -> b.nodeStateTransitionTimeout = nodeStateTransitionTimeout);
        }

        public Builder nodeRestartRetryAttempts(int nodeRestartRetryAttempts)
        {
            return update(b -> b.nodeRestartRetryAttempts = nodeRestartRetryAttempts);
        }

        public Builder waitBetweenExecutionGroups(SecondBoundConfiguration waitBetweenExecutionGroups)
        {
            return update(b -> b.waitBetweenExecutionGroups = waitBetweenExecutionGroups);
        }

        @Override
        public RollingRestartConfigurationImpl build()
        {
            return new RollingRestartConfigurationImpl(this);
        }
    }
}
