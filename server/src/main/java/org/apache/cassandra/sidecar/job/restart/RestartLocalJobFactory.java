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

import java.net.UnknownHostException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.common.data.OperationType;
import org.apache.cassandra.sidecar.common.response.RingResponse;
import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.RollingRestartConfiguration;
import org.apache.cassandra.sidecar.job.LocalJob;
import org.apache.cassandra.sidecar.job.LocalJobFactory;
import org.apache.cassandra.sidecar.lifecycle.LifecycleManager;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

/**
 * Factory for creating {@link RestartJob} instances. Handles RESTART operation type only.
 */
@Singleton
public class RestartLocalJobFactory implements LocalJobFactory
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RestartLocalJobFactory.class);
    private static final long EXECUTION_TIMEOUT_MARGIN_SECONDS = 60;

    private final InstanceMetadataFetcher metadataFetcher;
    private final LifecycleManager lifecycleManager;
    private final RingHealthChecker healthChecker;
    private final RollingRestartConfiguration config;

    @Inject
    public RestartLocalJobFactory(InstanceMetadataFetcher metadataFetcher,
                                  LifecycleManager lifecycleManager,
                                  RingHealthChecker healthChecker,
                                  RollingRestartConfiguration config)
    {
        this.metadataFetcher = metadataFetcher;
        this.lifecycleManager = lifecycleManager;
        this.healthChecker = healthChecker;
        this.config = config;
    }

    @Override
    public LocalJob createJob(UUID operationId, UUID nodeId, String instanceHost, OperationType operationType)
    {
        if (operationType != OperationType.RESTART)
        {
            throw new UnsupportedOperationException("RestartLocalJobFactory only supports RESTART, got " + operationType);
        }

        LOGGER.info("Creating RestartJob for node {}. operationId={} instanceHost={}", nodeId, operationId, instanceHost);

        // The ring is read inside the job, so a failure to read it is retried by the job and does not fail createJob
        return new RestartJob(operationId, nodeId, instanceHost, operationType,
                              lifecycleManager, healthChecker,
                              config.nodeRestartRetryAttempts(),
                              config.cassandraHealthTimeout(),
                              config.nodeStateTransitionTimeout(),
                              () -> ring(instanceHost));
    }

    @Override
    public SecondBoundConfiguration waitBetweenExecutionGroups(Map<String, String> operationMetadata)
    {
        return config.waitBetweenExecutionGroups();
    }

    @Override
    public boolean requiresPredecessorSuccess()
    {
        return true;
    }

    @Override
    public boolean canRecover()
    {
        return true;
    }

    @Override
    public SecondBoundConfiguration nodeExecutionTimeout()
    {
        long worstCaseSeconds =
            (config.cassandraHealthTimeout().toSeconds() + config.nodeStateTransitionTimeout().toSeconds() * 2)
            * (config.nodeRestartRetryAttempts() + 1);
        return new SecondBoundConfiguration(worstCaseSeconds + EXECUTION_TIMEOUT_MARGIN_SECONDS, TimeUnit.SECONDS);
    }

    private RingResponse ring(String instanceHost)
    {
        try
        {
            return metadataFetcher.instance(instanceHost).delegate().storageOperations().ring(null);
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException("Failed to query ring for " + instanceHost, e);
        }
    }
}
