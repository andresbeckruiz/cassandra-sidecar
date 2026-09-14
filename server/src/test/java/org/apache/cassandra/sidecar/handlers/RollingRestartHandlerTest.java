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

package org.apache.cassandra.sidecar.handlers;

import java.net.UnknownHostException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.util.Modules;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.common.response.OperationalJobResponse;
import org.apache.cassandra.sidecar.common.response.RingResponse;
import org.apache.cassandra.sidecar.common.response.data.RingEntry;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.config.RollingRestartConfiguration;
import org.apache.cassandra.sidecar.config.yaml.RollingRestartConfigurationImpl;
import org.apache.cassandra.sidecar.job.InMemoryOperationalJobTracker;
import org.apache.cassandra.sidecar.job.OperationalJobCoordinator;
import org.apache.cassandra.sidecar.job.OperationalJobTracker;
import org.apache.cassandra.sidecar.job.restart.ReplicaScope;
import org.apache.cassandra.sidecar.job.restart.ReplicationStrategyAnalyzer;
import org.apache.cassandra.sidecar.job.restart.RestartOperationMetadata;
import org.apache.cassandra.sidecar.job.restart.RollingRestartJob;
import org.apache.cassandra.sidecar.modules.SidecarModules;
import org.apache.cassandra.sidecar.server.Server;

import static io.netty.handler.codec.http.HttpResponseStatus.ACCEPTED;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.SERVICE_UNAVAILABLE;
import static org.apache.cassandra.sidecar.common.data.OperationalJobStatus.FAILED;
import static org.apache.cassandra.sidecar.common.data.OperationalJobStatus.RUNNING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link RollingRestartHandler}
 */
@ExtendWith(VertxExtension.class)
public class RollingRestartHandlerTest
{
    static final Logger LOGGER = LoggerFactory.getLogger(RollingRestartHandlerTest.class);

    static final String ROUTE = "/api/v1/cassandra/operations/restart";
    static final UUID NODE_1 = UUID.fromString("00000000-0000-0000-0000-000000000001");
    static final UUID NODE_2 = UUID.fromString("00000000-0000-0000-0000-000000000002");
    static final UUID NODE_3 = UUID.fromString("00000000-0000-0000-0000-000000000003");
    static final UUID NODE_4 = UUID.fromString("00000000-0000-0000-0000-000000000004");

    Vertx vertx;
    Server server;
    StorageOperations mockStorageOperations = mock(StorageOperations.class);
    OperationalJobCoordinator mockCoordinator = mock(OperationalJobCoordinator.class);
    InMemoryOperationalJobTracker jobTracker = new InMemoryOperationalJobTracker(10);
    RollingRestartConfiguration rollingRestartConfig = RollingRestartConfigurationImpl.builder()
                                                                                     .enabled(true)
                                                                                     .build();
    ReplicationStrategyAnalyzer mockStrategyAnalyzer = mock(ReplicationStrategyAnalyzer.class);

    @BeforeEach
    void before() throws InterruptedException, UnknownHostException
    {
        when(mockStorageOperations.operationMode()).thenReturn("NORMAL");
        when(mockStorageOperations.ring(null)).thenReturn(buildRingResponse());
        when(mockCoordinator.trySetActive(any(), any(), any())).thenReturn(true);
        // Default to N so parallelism > 1 is allowed; unsafe-topology tests override this.
        when(mockStrategyAnalyzer.replicaScopeForDatacenter(any(), any())).thenReturn(ReplicaScope.RACK);

        Module testOverride = Modules.override(new TestModule())
                                     .with(new CommonTest.CommonTestModule(mockStorageOperations));
        Module withRestartConfig = Modules.override(testOverride)
                                          .with(new RollingRestartTestModule());

        Injector injector = Guice.createInjector(Modules.override(SidecarModules.all())
                                                        .with(withRestartConfig));
        vertx = injector.getInstance(Vertx.class);
        server = injector.getInstance(Server.class);
        VertxTestContext context = new VertxTestContext();
        server.start()
              .onSuccess(s -> context.completeNow())
              .onFailure(context::failNow);
        context.awaitCompletion(5, TimeUnit.SECONDS);
    }

    @AfterEach
    void after() throws InterruptedException
    {
        CountDownLatch closeLatch = new CountDownLatch(1);
        server.close().onSuccess(res -> closeLatch.countDown());
        if (closeLatch.await(60, TimeUnit.SECONDS))
            LOGGER.info("Close event received before timeout.");
        else
            LOGGER.error("Close event timed out.");
    }

    @Test
    void testFeatureDisabledReturns503(VertxTestContext context)
    {
        rollingRestartConfig = RollingRestartConfigurationImpl.builder().enabled(false).build();
        reinitializeServer();

        WebClient client = WebClient.create(vertx);
        String body = "{\"datacenter\":\"dc1\"}";
        client.post(server.actualPort(), "127.0.0.1", ROUTE)
              .expect(ResponsePredicate.SC_SERVICE_UNAVAILABLE)
              .sendBuffer(Buffer.buffer(body), context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(SERVICE_UNAVAILABLE.code());
                  context.completeNow();
              }));
    }

    @Test
    void testMissingDatacenterReturns400(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String body = "{\"nodes\":[]}";
        client.post(server.actualPort(), "127.0.0.1", ROUTE)
              .expect(ResponsePredicate.SC_BAD_REQUEST)
              .sendBuffer(Buffer.buffer(body), context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(BAD_REQUEST.code());
                  context.completeNow();
              }));
    }

    @Test
    void testBlankDatacenterReturns400(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String body = "{\"datacenter\":\"  \"}";
        client.post(server.actualPort(), "127.0.0.1", ROUTE)
              .expect(ResponsePredicate.SC_BAD_REQUEST)
              .sendBuffer(Buffer.buffer(body), context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(BAD_REQUEST.code());
                  context.completeNow();
              }));
    }

    @Test
    void testUnknownDatacenterReturns400(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String body = "{\"datacenter\":\"dc_nonexistent\"}";
        client.post(server.actualPort(), "127.0.0.1", ROUTE)
              .expect(ResponsePredicate.SC_BAD_REQUEST)
              .sendBuffer(Buffer.buffer(body), context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(BAD_REQUEST.code());
                  context.completeNow();
              }));
    }

    @Test
    void testInvalidNodeUuidReturns400(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String body = "{\"datacenter\":\"dc1\",\"nodes\":[\"not-a-uuid\"]}";
        client.post(server.actualPort(), "127.0.0.1", ROUTE)
              .expect(ResponsePredicate.SC_BAD_REQUEST)
              .sendBuffer(Buffer.buffer(body), context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(BAD_REQUEST.code());
                  context.completeNow();
              }));
    }

    @Test
    void testNodeNotInDatacenterReturns400(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String unknownNode = UUID.randomUUID().toString();
        String body = "{\"datacenter\":\"dc1\",\"nodes\":[\"" + unknownNode + "\"]}";
        client.post(server.actualPort(), "127.0.0.1", ROUTE)
              .expect(ResponsePredicate.SC_BAD_REQUEST)
              .sendBuffer(Buffer.buffer(body), context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(BAD_REQUEST.code());
                  context.completeNow();
              }));
    }

    @Test
    void testMaxRestartParallelismExceedingRackSizeSucceeds(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        // maxRestartParallelism is an upper bound: dc1 racks have 2 nodes each, so maxRestartParallelism=3 is allowed and
        // simply restarts each rack's nodes together.
        String body = "{\"datacenter\":\"dc1\",\"maxRestartParallelism\":3}";
        client.post(server.actualPort(), "127.0.0.1", ROUTE)
              .sendBuffer(Buffer.buffer(body), context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(ACCEPTED.code());
                  OperationalJobResponse jobResponse = response.bodyAsJson(OperationalJobResponse.class);
                  assertThat(jobResponse).isNotNull();
                  assertThat(jobResponse.jobId()).isNotNull();
                  RollingRestartJob job = getSubmittedJob(jobResponse);
                  assertThat(job).isNotNull();
                  assertThat(job.nodeExecutionOrder()).allSatisfy(group -> assertThat(group).hasSize(2));
                  assertThat(job.operationMetadata())
                  .doesNotContainKey("maxRestartParallelism")
                  .containsEntry(RestartOperationMetadata.EFFECTIVE_MAX_RESTART_PARALLELISM, "2");
                  context.completeNow();
              }));
    }

    @Test
    void testMaxRestartParallelismUnsafeForReplicationStrategyCapsToSerial(VertxTestContext context)
    {
        when(mockStrategyAnalyzer.replicaScopeForDatacenter(any(), any())).thenReturn(ReplicaScope.CLUSTER);
        reinitializeServer();

        WebClient client = WebClient.create(vertx);
        String body = "{\"datacenter\":\"dc1\",\"maxRestartParallelism\":3}";
        client.post(server.actualPort(), "127.0.0.1", ROUTE)
              .sendBuffer(Buffer.buffer(body), context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(ACCEPTED.code());
                  OperationalJobResponse jobResponse = response.bodyAsJson(OperationalJobResponse.class);
                  RollingRestartJob job = getSubmittedJob(jobResponse);
                  assertThat(job).isNotNull();
                  // Capped to serial: every execution group holds exactly one node.
                  assertThat(job.nodeExecutionOrder()).allSatisfy(group -> assertThat(group).hasSize(1));
                  assertThat(job.operationMetadata())
                  .doesNotContainKey("maxRestartParallelism")
                  .containsEntry(RestartOperationMetadata.EFFECTIVE_MAX_RESTART_PARALLELISM, "1");
                  context.completeNow();
              }));
    }

    @Test
    void testSingleNodeParallelismAllowedOnUnsafeReplicationStrategy(VertxTestContext context)
    {
        // Serial restarts are always safe, so an unsafe replication strategy must not block maxRestartParallelism=1.
        when(mockStrategyAnalyzer.replicaScopeForDatacenter(any(), any())).thenReturn(ReplicaScope.CLUSTER);
        reinitializeServer();

        WebClient client = WebClient.create(vertx);
        String body = "{\"datacenter\":\"dc1\",\"maxRestartParallelism\":1}";
        client.post(server.actualPort(), "127.0.0.1", ROUTE)
              .sendBuffer(Buffer.buffer(body), context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(ACCEPTED.code());
                  context.completeNow();
              }));
    }

    @Test
    void testMaxRestartParallelismZeroReturns400(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String body = "{\"datacenter\":\"dc1\",\"maxRestartParallelism\":0}";
        client.post(server.actualPort(), "127.0.0.1", ROUTE)
              .expect(ResponsePredicate.SC_BAD_REQUEST)
              .sendBuffer(Buffer.buffer(body), context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(BAD_REQUEST.code());
                  context.completeNow();
              }));
    }

    @Test
    void testNonPositiveOverrideReturns400(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        // A zero/negative override for any of the optional timeout/retry fields is rejected; here nodeStateTransitionTimeoutSeconds=0.
        String body = "{\"datacenter\":\"dc1\",\"nodeStateTransitionTimeoutSeconds\":0}";
        client.post(server.actualPort(), "127.0.0.1", ROUTE)
              .expect(ResponsePredicate.SC_BAD_REQUEST)
              .sendBuffer(Buffer.buffer(body), context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(BAD_REQUEST.code());
                  context.completeNow();
              }));
    }

    @Test
    void testNegativeRetryAttemptsReturns400(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String body = "{\"datacenter\":\"dc1\",\"nodeRestartRetryAttempts\":-1}";
        client.post(server.actualPort(), "127.0.0.1", ROUTE)
              .expect(ResponsePredicate.SC_BAD_REQUEST)
              .sendBuffer(Buffer.buffer(body), context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(BAD_REQUEST.code());
                  context.completeNow();
              }));
    }

    @Test
    void testZeroRetryAttemptsAcceptedAndPropagatedToMetadata(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        // nodeRestartRetryAttempts counts retries, so 0 (restart once, no retry) is valid.
        String body = "{\"datacenter\":\"dc1\",\"nodeRestartRetryAttempts\":0}";
        client.post(server.actualPort(), "127.0.0.1", ROUTE)
              .sendBuffer(Buffer.buffer(body), context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(ACCEPTED.code());
                  OperationalJobResponse jobResponse = response.bodyAsJson(OperationalJobResponse.class);
                  RollingRestartJob job = getSubmittedJob(jobResponse);
                  assertThat(job).isNotNull();
                  assertThat(job.operationMetadata())
                  .containsEntry(RestartOperationMetadata.NODE_RESTART_RETRY_ATTEMPTS, "0");
                  context.completeNow();
              }));
    }

    @Test
    void testNegativeWaitBetweenExecutionGroupsReturns400(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String body = "{\"datacenter\":\"dc1\",\"waitBetweenExecutionGroupsSeconds\":-1}";
        client.post(server.actualPort(), "127.0.0.1", ROUTE)
              .expect(ResponsePredicate.SC_BAD_REQUEST)
              .sendBuffer(Buffer.buffer(body), context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(BAD_REQUEST.code());
                  context.completeNow();
              }));
    }

    @Test
    void testZeroWaitBetweenExecutionGroupsAcceptedAndPropagatedToMetadata(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        // Zero is a meaningful value here: it opts this restart out of the inter-group settle wait.
        String body = "{\"datacenter\":\"dc1\",\"waitBetweenExecutionGroupsSeconds\":0}";
        client.post(server.actualPort(), "127.0.0.1", ROUTE)
              .sendBuffer(Buffer.buffer(body), context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(ACCEPTED.code());
                  OperationalJobResponse jobResponse = response.bodyAsJson(OperationalJobResponse.class);
                  RollingRestartJob job = getSubmittedJob(jobResponse);
                  assertThat(job).isNotNull();
                  assertThat(job.operationMetadata())
                  .containsEntry(RestartOperationMetadata.WAIT_BETWEEN_EXECUTION_GROUPS_SECONDS, "0");
                  context.completeNow();
              }));
    }

    @Test
    void testWaitBetweenExecutionGroupsAboveMaximumReturns400(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        // A millisecond count passed where seconds are expected is the mistake the upper bound exists to catch.
        String body = "{\"datacenter\":\"dc1\",\"waitBetweenExecutionGroupsSeconds\":180000}";
        client.post(server.actualPort(), "127.0.0.1", ROUTE)
              .expect(ResponsePredicate.SC_BAD_REQUEST)
              .sendBuffer(Buffer.buffer(body), context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(BAD_REQUEST.code());
                  context.completeNow();
              }));
    }

    @Test
    void testWaitBetweenExecutionGroupsAtMaximumAccepted(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String body = "{\"datacenter\":\"dc1\",\"waitBetweenExecutionGroupsSeconds\":"
                      + RollingRestartHandler.MAX_WAIT_BETWEEN_EXECUTION_GROUPS_SECONDS + "}";
        client.post(server.actualPort(), "127.0.0.1", ROUTE)
              .sendBuffer(Buffer.buffer(body), context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(ACCEPTED.code());
                  OperationalJobResponse jobResponse = response.bodyAsJson(OperationalJobResponse.class);
                  RollingRestartJob job = getSubmittedJob(jobResponse);
                  assertThat(job).isNotNull();
                  assertThat(job.operationMetadata())
                  .containsEntry(RestartOperationMetadata.WAIT_BETWEEN_EXECUTION_GROUPS_SECONDS,
                                 String.valueOf(RollingRestartHandler.MAX_WAIT_BETWEEN_EXECUTION_GROUPS_SECONDS));
                  context.completeNow();
              }));
    }

    @Test
    void testDuplicateNodeIdsReturns400(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String body = "{\"datacenter\":\"dc1\",\"nodes\":[\"" + NODE_1 + "\",\"" + NODE_1 + "\"]}";
        client.post(server.actualPort(), "127.0.0.1", ROUTE)
              .expect(ResponsePredicate.SC_BAD_REQUEST)
              .sendBuffer(Buffer.buffer(body), context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(BAD_REQUEST.code());
                  context.completeNow();
              }));
    }

    @Test
    void testSuccessfulSubmitEmptyNodesAndResolvesToEveryDCNode(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String body = "{\"datacenter\":\"dc1\"}";
        client.post(server.actualPort(), "127.0.0.1", ROUTE)
              .sendBuffer(Buffer.buffer(body), context.succeeding(response -> {
                  int statusCode = response.statusCode();
                  assertThat(statusCode).isEqualTo(ACCEPTED.code());
                  OperationalJobResponse jobResponse = response.bodyAsJson(OperationalJobResponse.class);
                  assertThat(jobResponse).isNotNull();
                  assertThat(jobResponse.jobId()).isNotNull();
                  assertThat(jobResponse.status()).isEqualTo(RUNNING);
                  RollingRestartJob job = getSubmittedJob(jobResponse);
                  assertThat(job).isNotNull();
                  assertThat(getJobTargetNodes(job)).containsExactlyInAnyOrder(NODE_1, NODE_2, NODE_3, NODE_4);
                  context.completeNow();
              }));
    }

    @Test
    void testExplicitNullNodesRestartsAll(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        // An explicit JSON null for nodes is normalized to an empty list, so it restarts every node in the datacenter.
        String body = "{\"datacenter\":\"dc1\",\"nodes\":null}";
        client.post(server.actualPort(), "127.0.0.1", ROUTE)
              .sendBuffer(Buffer.buffer(body), context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(ACCEPTED.code());
                  OperationalJobResponse jobResponse = response.bodyAsJson(OperationalJobResponse.class);
                  assertThat(jobResponse).isNotNull();
                  assertThat(jobResponse.jobId()).isNotNull();
                  assertThat(jobResponse.status()).isEqualTo(RUNNING);
                  context.completeNow();
              }));
    }

    @Test
    void testSuccessfulSubmitExplicitNodes(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String body = "{\"datacenter\":\"dc1\",\"nodes\":[\"" + NODE_1 + "\",\"" + NODE_3 + "\"]}";
        client.post(server.actualPort(), "127.0.0.1", ROUTE)
              .sendBuffer(Buffer.buffer(body), context.succeeding(response -> {
                  int statusCode = response.statusCode();
                  assertThat(statusCode).isEqualTo(ACCEPTED.code());
                  OperationalJobResponse jobResponse = response.bodyAsJson(OperationalJobResponse.class);
                  assertThat(jobResponse).isNotNull();
                  assertThat(jobResponse.jobId()).isNotNull();
                  RollingRestartJob job = getSubmittedJob(jobResponse);
                  assertThat(job).isNotNull();
                  assertThat(getJobTargetNodes(job)).containsExactlyInAnyOrder(NODE_1, NODE_3);
                  assertThat(job.nodeExecutionOrder()).hasSize(2);
                  assertThat(job.nodeExecutionOrder()).allSatisfy(group -> assertThat(group).hasSize(1));
                  context.completeNow();
              }));
    }

    @Test
    void testMalformedJsonReturns400(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String body = "{invalid json}";
        client.post(server.actualPort(), "127.0.0.1", ROUTE)
              .expect(ResponsePredicate.SC_BAD_REQUEST)
              .sendBuffer(Buffer.buffer(body), context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(BAD_REQUEST.code());
                  context.completeNow();
              }));
    }

    @Test
    void testRingRetrievalFailureReturns500(VertxTestContext context) throws UnknownHostException
    {
        when(mockStorageOperations.ring(null)).thenThrow(new UnknownHostException("simulated ring failure"));

        WebClient client = WebClient.create(vertx);
        String body = "{\"datacenter\":\"dc1\"}";
        client.post(server.actualPort(), "127.0.0.1", ROUTE)
              .expect(ResponsePredicate.SC_INTERNAL_SERVER_ERROR)
              .sendBuffer(Buffer.buffer(body), context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(INTERNAL_SERVER_ERROR.code());
                  context.completeNow();
              }));
    }

    @Test
    void testConflictReturns409(VertxTestContext context)
    {
        when(mockCoordinator.trySetActive(any(), any(), any())).thenReturn(false);

        WebClient client = WebClient.create(vertx);
        String body = "{\"datacenter\":\"dc1\"}";
        client.post(server.actualPort(), "127.0.0.1", ROUTE)
              .expect(ResponsePredicate.SC_CONFLICT)
              .sendBuffer(Buffer.buffer(body), context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(CONFLICT.code());
                  OperationalJobResponse jobResponse = response.bodyAsJson(OperationalJobResponse.class);
                  assertThat(jobResponse).isNotNull();
                  assertThat(jobResponse.status()).isEqualTo(FAILED);
                  assertThat(jobResponse.reason()).contains("An active operation already exists");
                  context.completeNow();
              }));
    }

    private RollingRestartJob getSubmittedJob(OperationalJobResponse response)
    {
        return (RollingRestartJob) jobTracker.jobsView().get(response.jobId());
    }

    private List<UUID> getJobTargetNodes(RollingRestartJob job)
    {
        return job.nodeExecutionOrder().stream().flatMap(List::stream).collect(Collectors.toList());
    }

    private RingResponse buildRingResponse()
    {
        RingResponse ring = new RingResponse();
        ring.add(ringEntry("dc1", "rack1", NODE_1.toString(), "1"));
        ring.add(ringEntry("dc1", "rack1", NODE_2.toString(), "2"));
        ring.add(ringEntry("dc1", "rack2", NODE_3.toString(), "3"));
        ring.add(ringEntry("dc1", "rack2", NODE_4.toString(), "4"));
        return ring;
    }

    private RingEntry ringEntry(String datacenter, String rack, String hostId, String token)
    {
        return new RingEntry(datacenter, "127.0.0.1", 9042, rack,
                             "Up", "Normal", "100 KiB", "100%",
                             token, "localhost", hostId);
    }

    private void reinitializeServer()
    {
        try
        {
            when(mockStorageOperations.ring(null)).thenReturn(buildRingResponse());
            CountDownLatch closeLatch = new CountDownLatch(1);
            server.close().onSuccess(res -> closeLatch.countDown());
            closeLatch.await(60, TimeUnit.SECONDS);

            Module testOverride = Modules.override(new TestModule())
                                         .with(new CommonTest.CommonTestModule(mockStorageOperations));
            Module withRestartConfig = Modules.override(testOverride)
                                              .with(new RollingRestartTestModule());
            Injector injector = Guice.createInjector(Modules.override(SidecarModules.all())
                                                            .with(withRestartConfig));
            vertx = injector.getInstance(Vertx.class);
            server = injector.getInstance(Server.class);
            VertxTestContext ctx = new VertxTestContext();
            server.start()
                  .onSuccess(s -> ctx.completeNow())
                  .onFailure(ctx::failNow);
            ctx.awaitCompletion(5, TimeUnit.SECONDS);
        }
        catch (InterruptedException | UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    class RollingRestartTestModule extends AbstractModule
    {
        @Provides
        @Singleton
        RollingRestartConfiguration rollingRestartConfiguration()
        {
            return rollingRestartConfig;
        }

        @Provides
        @Singleton
        OperationalJobCoordinator operationalJobCoordinator()
        {
            return mockCoordinator;
        }

        @Provides
        @Singleton
        OperationalJobTracker operationalJobTracker()
        {
            return jobTracker;
        }

        @Provides
        @Singleton
        ReplicationStrategyAnalyzer replicationStrategyAnalyzer()
        {
            return mockStrategyAnalyzer;
        }
    }
}
