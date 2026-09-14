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

package org.apache.cassandra.sidecar.routes;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.extension.ExtendWith;

import com.datastax.driver.core.utils.UUIDs;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.common.ApiEndpointsV1;
import org.apache.cassandra.sidecar.common.data.OperationType;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.apache.cassandra.sidecar.common.response.OperationalJobResponse;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.job.DurableOperationalJobTracker;
import org.apache.cassandra.sidecar.job.OperationalJobTracker;
import org.apache.cassandra.sidecar.job.storage.OperationalJobRecord;
import org.apache.cassandra.sidecar.job.storage.StorageProvider;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.testing.CassandraIntegrationTest;

import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.OPERATIONAL_JOB_ID_PATH_PARAM;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises {@code PATCH /api/v1/cassandra/operational-jobs/:operationId} against a real Cassandra provider
 */
@ExtendWith(VertxExtension.class)
class UpdateOperationalJobIntegrationTest extends IntegrationTestBase
{
    private static final JsonObject ABORT_REQUEST = patch("replace", "/status", "ABORTED");

    /**
     * Aborting is a durable transition that the Sidecars running the operation observe from storage, so it is only
     * offered by a tracker that stores job state durably.
     */
    @Override
    protected void beforeSetup()
    {
        installTestSpecificModule(new AbstractModule()
        {
            @Provides
            @Singleton
            OperationalJobTracker durableTracker(ServiceConfiguration serviceConfiguration,
                                                 StorageProvider storageProvider,
                                                 ExecutorPools executorPools)
            {
                return new DurableOperationalJobTracker(serviceConfiguration, storageProvider,
                                                        executorPools.service());
            }
        });
    }

    @CassandraIntegrationTest
    void abortingWedgedJobWritesOnlyTheRecordAndLeavesItsLockHeld(VertxTestContext context) throws Exception
    {
        waitForSchemaReady(30, TimeUnit.SECONDS);
        StorageProvider storage = injector.getInstance(StorageProvider.class);
        storage.initialize();

        UUID jobId = UUIDs.timeBased();
        UUID completedNode = UUID.randomUUID();
        UUID wedgedNode = UUID.randomUUID();
        UUID pendingNode = UUID.randomUUID();

        // Group 0 half done: one node finished, one is stuck RUNNING. Group 1 never started because it is waiting
        // on group 0
        storage.persistJob(OperationalJobRecord.builder()
                                               .jobId(jobId)
                                               .operationType(OperationType.RESTART)
                                               .status(OperationalJobStatus.RUNNING)
                                               .nodeExecutionOrder(Arrays.asList(Arrays.asList(completedNode, wedgedNode),
                                                                                   Collections.singletonList(pendingNode)))
                                               .build());
        assertThat(storage.trySetActiveOperation(OperationType.RESTART, jobId, null)).isTrue();
        storage.updateNodeStatus(jobId, completedNode, OperationalJobStatus.SUCCEEDED);
        storage.updateNodeStatus(jobId, wedgedNode, OperationalJobStatus.RUNNING);
        storage.updateNodeStatus(jobId, pendingNode, OperationalJobStatus.CREATED);

        testWithClient(client -> client.patch(server.actualPort(), "127.0.0.1", routeFor(jobId))
                                       .sendJsonObject(ABORT_REQUEST, context.succeeding(response -> {
                                           assertThat(response.statusCode())
                                           .describedAs("the record is written, and the cluster still has to wind "
                                                        + "the operation down")
                                           .isEqualTo(HttpResponseStatus.ACCEPTED.code());
                                           OperationalJobResponse job =
                                           response.bodyAsJson(OperationalJobResponse.class);
                                           assertThat(job.status()).isEqualTo(OperationalJobStatus.ABORTED);
                                           assertThat(job.reason()).contains("operator request");
                                           context.completeNow();
                                       })));
        assertThat(context.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();

        OperationalJobRecord updated = storage.findJob(jobId);
        assertThat(updated).isNotNull();
        assertThat(updated.status()).isEqualTo(OperationalJobStatus.ABORTED);
        assertThat(updated.failureReason())
        .describedAs("the server-written reason is what makes a job aborted by hand distinguishable afterwards")
        .contains("operator request");

        Map<UUID, OperationalJobStatus> nodeStatuses = storage.getNodeStatusesForOperation(jobId);
        assertThat(nodeStatuses.get(completedNode)).isEqualTo(OperationalJobStatus.SUCCEEDED);
        assertThat(nodeStatuses.get(wedgedNode))
        .describedAs("a row this Sidecar does not own must be left to the Sidecar that does; settling it here "
                     + "would let the lock drop while that node's job is still running")
        .isEqualTo(OperationalJobStatus.RUNNING);
        assertThat(nodeStatuses.get(pendingNode)).isEqualTo(OperationalJobStatus.CREATED);

        assertThat(storage.getActiveOperation(OperationType.RESTART))
        .describedAs("the handler must never release the lock; finalization does that once every node row is "
                     + "terminal, which cannot happen while a job is still live")
        .isEqualTo(jobId);
    }

    @CassandraIntegrationTest
    void forceSettlesOutstandingRowsButStillLeavesTheLockHeld(VertxTestContext context) throws Exception
    {
        waitForSchemaReady(30, TimeUnit.SECONDS);
        StorageProvider storage = injector.getInstance(StorageProvider.class);
        storage.initialize();

        UUID jobId = UUIDs.timeBased();
        UUID completedNode = UUID.randomUUID();
        UUID wedgedNode = UUID.randomUUID();
        UUID pendingNode = UUID.randomUUID();

        storage.persistJob(OperationalJobRecord.builder()
                                               .jobId(jobId)
                                               .operationType(OperationType.RESTART)
                                               .status(OperationalJobStatus.RUNNING)
                                               .nodeExecutionOrder(Arrays.asList(Arrays.asList(completedNode, wedgedNode),
                                                                                   Collections.singletonList(pendingNode)))
                                               .build());
        assertThat(storage.trySetActiveOperation(OperationType.RESTART, jobId, null)).isTrue();
        storage.updateNodeStatus(jobId, completedNode, OperationalJobStatus.SUCCEEDED);
        storage.updateNodeStatus(jobId, wedgedNode, OperationalJobStatus.RUNNING);
        storage.updateNodeStatus(jobId, pendingNode, OperationalJobStatus.CREATED);

        testWithClient(client -> client.patch(server.actualPort(), "127.0.0.1", forcedRouteFor(jobId))
                                       .sendJsonObject(ABORT_REQUEST, context.succeeding(response -> {
                                           assertThat(response.statusCode())
                                           .isEqualTo(HttpResponseStatus.ACCEPTED.code());
                                           context.completeNow();
                                       })));
        assertThat(context.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();

        Map<UUID, OperationalJobStatus> nodeStatuses = storage.getNodeStatusesForOperation(jobId);
        assertThat(nodeStatuses.get(completedNode))
        .describedAs("a node that genuinely finished is left alone even under force")
        .isEqualTo(OperationalJobStatus.SUCCEEDED);
        assertThat(nodeStatuses.get(wedgedNode))
        .describedAs("this node was executing, so it may be left mid-operation with nobody to reconcile it; "
                     + "FAILED tells the operator to investigate it")
        .isEqualTo(OperationalJobStatus.FAILED);
        assertThat(nodeStatuses.get(pendingNode))
        .describedAs("this node never started, so ABORTED still means untouched")
        .isEqualTo(OperationalJobStatus.ABORTED);

        assertThat(storage.getActiveOperation(OperationType.RESTART))
        .describedAs("force settles rows only; the lock is still released by finalization, never by the handler")
        .isEqualTo(jobId);
    }

    @CassandraIntegrationTest
    void forceOnAnAlreadyAbortedRecordSettlesItsRows(VertxTestContext context) throws Exception
    {
        waitForSchemaReady(30, TimeUnit.SECONDS);
        StorageProvider storage = injector.getInstance(StorageProvider.class);
        storage.initialize();

        UUID jobId = UUIDs.timeBased();
        UUID node = UUID.randomUUID();

        // The expected operator flow: abort, wait for the cluster to converge, and only force once it is clear
        // that some Sidecar is never going to settle its own row.
        storage.persistJob(OperationalJobRecord.builder()
                                               .jobId(jobId)
                                               .operationType(OperationType.RESTART)
                                               .status(OperationalJobStatus.ABORTED)
                                               .failureReason("Aborted by operator request")
                                               .nodeExecutionOrder(Collections.singletonList(Collections.singletonList(node)))
                                               .build());
        assertThat(storage.trySetActiveOperation(OperationType.RESTART, jobId, null)).isTrue();
        storage.updateNodeStatus(jobId, node, OperationalJobStatus.RUNNING);

        testWithClient(client -> client.patch(server.actualPort(), "127.0.0.1", forcedRouteFor(jobId))
                                       .sendJsonObject(ABORT_REQUEST, context.succeeding(response -> {
                                           assertThat(response.statusCode())
                                           .isEqualTo(HttpResponseStatus.ACCEPTED.code());
                                           context.completeNow();
                                       })));
        assertThat(context.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();

        assertThat(storage.getNodeStatusesForOperation(jobId).get(node)).isEqualTo(OperationalJobStatus.FAILED);
    }

    @CassandraIntegrationTest
    void reapplyingToAnAlreadyAbortedJobConverges(VertxTestContext context) throws Exception
    {
        waitForSchemaReady(30, TimeUnit.SECONDS);
        StorageProvider storage = injector.getInstance(StorageProvider.class);
        storage.initialize();

        UUID jobId = UUIDs.timeBased();
        UUID node = UUID.randomUUID();

        storage.persistJob(OperationalJobRecord.builder()
                                               .jobId(jobId)
                                               .operationType(OperationType.RESTART)
                                               .status(OperationalJobStatus.ABORTED)
                                               .failureReason("Aborted by operator request")
                                               .nodeExecutionOrder(Collections.singletonList(Collections.singletonList(node)))
                                               .build());
        assertThat(storage.trySetActiveOperation(OperationType.RESTART, jobId, null)).isTrue();
        storage.updateNodeStatus(jobId, node, OperationalJobStatus.RUNNING);

        testWithClient(client -> client.patch(server.actualPort(), "127.0.0.1", routeFor(jobId))
                                       .sendJsonObject(ABORT_REQUEST, context.succeeding(response -> {
                                           assertThat(response.statusCode())
                                           .describedAs("an operator who cannot tell whether the first request "
                                                        + "landed must be able to simply repeat it")
                                           .isEqualTo(HttpResponseStatus.ACCEPTED.code());
                                           context.completeNow();
                                       })));
        assertThat(context.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();

        assertThat(storage.findJob(jobId).status()).isEqualTo(OperationalJobStatus.ABORTED);
        assertThat(storage.getNodeStatusesForOperation(jobId).get(node)).isEqualTo(OperationalJobStatus.RUNNING);
    }

    @CassandraIntegrationTest
    void abortsJobThatHoldsNoLock(VertxTestContext context) throws Exception
    {
        waitForSchemaReady(30, TimeUnit.SECONDS);
        StorageProvider storage = injector.getInstance(StorageProvider.class);
        storage.initialize();

        UUID jobId = UUIDs.timeBased();
        UUID node = UUID.randomUUID();

        // A record left non-terminal with its lock already gone is invisible to the poll loop, so refusing this
        // would strand exactly the records that most need repairing.
        storage.persistJob(OperationalJobRecord.builder()
                                               .jobId(jobId)
                                               .operationType(OperationType.RESTART)
                                               .status(OperationalJobStatus.RUNNING)
                                               .nodeExecutionOrder(Collections.singletonList(Collections.singletonList(node)))
                                               .build());
        storage.updateNodeStatus(jobId, node, OperationalJobStatus.RUNNING);

        testWithClient(client -> client.patch(server.actualPort(), "127.0.0.1", routeFor(jobId))
                                       .sendJsonObject(ABORT_REQUEST, context.succeeding(response -> {
                                           assertThat(response.statusCode())
                                           .isEqualTo(HttpResponseStatus.ACCEPTED.code());
                                           context.completeNow();
                                       })));
        assertThat(context.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();

        assertThat(storage.findJob(jobId).status()).isEqualTo(OperationalJobStatus.ABORTED);
    }

    @CassandraIntegrationTest
    void rejectsAbortingAJobThatAlreadySucceeded(VertxTestContext context) throws Exception
    {
        waitForSchemaReady(30, TimeUnit.SECONDS);
        StorageProvider storage = injector.getInstance(StorageProvider.class);
        storage.initialize();

        UUID jobId = UUIDs.timeBased();
        UUID node = UUID.randomUUID();

        storage.persistJob(OperationalJobRecord.builder()
                                               .jobId(jobId)
                                               .operationType(OperationType.RESTART)
                                               .status(OperationalJobStatus.SUCCEEDED)
                                               .nodeExecutionOrder(Collections.singletonList(Collections.singletonList(node)))
                                               .build());

        testWithClient(client -> client.patch(server.actualPort(), "127.0.0.1", routeFor(jobId))
                                       .sendJsonObject(ABORT_REQUEST, context.succeeding(response -> {
                                           assertThat(response.statusCode())
                                           .describedAs("the work is already done, so there is nothing to abort")
                                           .isEqualTo(HttpResponseStatus.CONFLICT.code());
                                           context.completeNow();
                                       })));
        assertThat(context.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();

        assertThat(storage.findJob(jobId).status()).isEqualTo(OperationalJobStatus.SUCCEEDED);
    }

    @CassandraIntegrationTest
    void rejectsJobWithoutExecutionOrderAsNotCoordinated(VertxTestContext context) throws Exception
    {
        waitForSchemaReady(30, TimeUnit.SECONDS);
        StorageProvider storage = injector.getInstance(StorageProvider.class);
        storage.initialize();

        UUID jobId = UUIDs.timeBased();
        // A single-node job has no execution order, so it never acquires a lock and has no node rows to settle.
        // Refused as a bad request rather than a conflict: that is a permanent property of the job, so unlike the
        // state-dependent refusals there is nothing for the operator to retry. Nothing may be written either.
        storage.persistJob(OperationalJobRecord.builder()
                                               .jobId(jobId)
                                               .operationType(OperationType.DECOMMISSION)
                                               .status(OperationalJobStatus.RUNNING)
                                               .build());

        testWithClient(client -> client.patch(server.actualPort(), "127.0.0.1", routeFor(jobId))
                                       .sendJsonObject(ABORT_REQUEST, context.succeeding(response -> {
                                           assertThat(response.statusCode())
                                           .isEqualTo(HttpResponseStatus.BAD_REQUEST.code());
                                           context.completeNow();
                                       })));
        assertThat(context.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();

        assertThat(storage.findJob(jobId).status()).isEqualTo(OperationalJobStatus.RUNNING);
    }

    @CassandraIntegrationTest
    void rejectsUnknownJob(VertxTestContext context) throws Exception
    {
        waitForSchemaReady(30, TimeUnit.SECONDS);

        testWithClient(client -> client.patch(server.actualPort(), "127.0.0.1", routeFor(UUIDs.timeBased()))
                                       .sendJsonObject(ABORT_REQUEST, context.succeeding(response -> {
                                           assertThat(response.statusCode())
                                           .isEqualTo(HttpResponseStatus.NOT_FOUND.code());
                                           context.completeNow();
                                       })));
        assertThat(context.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @CassandraIntegrationTest
    void rejectsMalformedRequests() throws Exception
    {
        // None of these reach storage, so the wait is not strictly needed. It is here so that a case added later
        // which does reach storage fails on its own merits rather than on an uninitialized schema.
        waitForSchemaReady(30, TimeUnit.SECONDS);

        List<MalformedRequest> cases = Arrays.asList(
        new MalformedRequest("no body at all", null),
        new MalformedRequest("a literal null body", Buffer.buffer("null")),
        new MalformedRequest("a body that is not JSON", Buffer.buffer("not json")),
        new MalformedRequest("an op other than replace", patch("add", "/status", "ABORTED").toBuffer()),
        new MalformedRequest("a path addressing a field other than the status",
                             patch("replace", "/reason", "ABORTED").toBuffer()),
        new MalformedRequest("an unrooted path instead of the JSON Pointer RFC 6902 specifies",
                             patch("replace", "status", "ABORTED").toBuffer()),
        new MalformedRequest("a transition to a status other than ABORTED",
                             patch("replace", "/status", "SUCCEEDED").toBuffer()),
        new MalformedRequest("a transition an operation may only reach on its own",
                             patch("replace", "/status", "FAILED").toBuffer()),
        new MalformedRequest("a status the Sidecar has no constant for",
                             patch("replace", "/status", "PAUSED").toBuffer()));

        for (MalformedRequest malformed : cases)
        {
            // A fresh context per case, so each has to be awaited through awaitSuccess rather than awaitCompletion.
            VertxTestContext context = new VertxTestContext();
            testWithClient(client -> {
                HttpRequest<Buffer> request =
                client.patch(server.actualPort(), "127.0.0.1", routeFor(UUIDs.timeBased()));
                Handler<AsyncResult<HttpResponse<Buffer>>> handler = context.succeeding(response -> {
                    assertThat(response.statusCode())
                    .describedAs("a request with %s must be rejected as a bad request, not surface as a server error",
                                 malformed.description)
                    .isEqualTo(HttpResponseStatus.BAD_REQUEST.code());
                    context.completeNow();
                });
                if (malformed.body == null)
                {
                    request.send(handler);
                }
                else
                {
                    request.sendBuffer(malformed.body, handler);
                }
            });
            awaitSuccess(context);
        }
    }

    /**
     * A body the endpoint must reject, with a description of what is wrong with it for the failure message.
     * A {@code null} body means the request is sent with no body at all.
     */
    private static final class MalformedRequest
    {
        private final String description;
        private final Buffer body;

        private MalformedRequest(String description, Buffer body)
        {
            this.description = description;
            this.body = body;
        }
    }

    /**
     * Awaits a {@link VertxTestContext} created by the test rather than injected. A failing assertion inside a
     * handler completes the context by failing it, so {@code awaitCompletion} returns {@code true} either way, and
     * only the injected context is inspected by {@link VertxExtension} afterwards. Without this, an assertion in
     * such a handler never fails the test.
     */
    private static void awaitSuccess(VertxTestContext context) throws Exception
    {
        assertThat(context.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
        if (context.failed())
        {
            throw new AssertionError(context.causeOfFailure());
        }
    }

    private static JsonObject patch(String op, String path, String value)
    {
        return new JsonObject().put("op", op).put("path", path).put("value", value);
    }

    private static String routeFor(UUID jobId)
    {
        return ApiEndpointsV1.OPERATIONAL_JOB_ROUTE.replace(OPERATIONAL_JOB_ID_PATH_PARAM, jobId.toString());
    }

    private static String forcedRouteFor(UUID jobId)
    {
        return routeFor(jobId) + "?force=true";
    }
}
