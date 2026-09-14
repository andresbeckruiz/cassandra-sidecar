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

import java.util.Collections;
import java.util.Set;
import java.util.UUID;

import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.Json;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.HttpException;
import org.apache.cassandra.sidecar.acl.authorization.BasicPermissions;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.apache.cassandra.sidecar.common.request.data.UpdateOperationalJobRequestPayload;
import org.apache.cassandra.sidecar.common.response.OperationalJobResponse;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.exceptions.OperationalJobConflictException;
import org.apache.cassandra.sidecar.exceptions.OperationalJobNotCoordinatedException;
import org.apache.cassandra.sidecar.exceptions.OperationalJobNotFoundException;
import org.apache.cassandra.sidecar.job.OperationalJobInfo;
import org.apache.cassandra.sidecar.job.OperationalJobManager;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.OPERATIONAL_JOB_ID_PATH_PARAM;
import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;
import static org.apache.cassandra.sidecar.utils.RequestUtils.parseBooleanQueryParam;

/**
 * Handles PATCH requests to {@code /api/v1/cassandra/operational-jobs/:operationId} to abort an operational job.
 *
 * <p>The body is an RFC 6902 JSON Patch of the job's status:
 * {@code {"op": "replace", "path": "/status", "value": "ABORTED"}}. {@code ABORTED} is the only value supported.</p>
 *
 * <p>The request may be sent to any Sidecar in the cluster and is idempotent.</p>
 */
public class UpdateOperationalJobHandler
extends AbstractHandler<UpdateOperationalJobHandler.AbortRequest> implements AccessProtected
{
    private final OperationalJobManager jobManager;

    @Inject
    public UpdateOperationalJobHandler(InstanceMetadataFetcher metadataFetcher,
                                       ExecutorPools executorPools,
                                       CassandraInputValidator validator,
                                       OperationalJobManager jobManager)
    {
        super(metadataFetcher, executorPools, validator);
        this.jobManager = jobManager;
    }

    @Override
    public Set<Authorization> requiredAuthorizations()
    {
        return Collections.singleton(BasicPermissions.UPDATE_OPERATIONAL_JOB.toAuthorization());
    }

    @Override
    protected AbortRequest extractParamsOrThrow(RoutingContext context)
    {
        UUID jobId = validatedJobId(context);

        String body = context.body().asString();
        if (body == null || body.equalsIgnoreCase("null")) // json encoder writes null as "null"
        {
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST,
                                    "Request body is required and must be a JSON patch of the form "
                                    + "{\"op\": \"" + UpdateOperationalJobRequestPayload.REPLACE_OP + "\", "
                                    + "\"path\": \"" + UpdateOperationalJobRequestPayload.STATUS_PATH + "\", "
                                    + "\"value\": \"" + OperationalJobStatus.ABORTED + "\"}");
        }

        UpdateOperationalJobRequestPayload payload;
        try
        {
            payload = Json.decodeValue(body, UpdateOperationalJobRequestPayload.class);
        }
        catch (DecodeException | IllegalArgumentException e)
        {
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST, "Invalid request body: " + e.getMessage());
        }

        if (!UpdateOperationalJobRequestPayload.REPLACE_OP.equals(payload.op()))
        {
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST,
                                    "op must be \"" + UpdateOperationalJobRequestPayload.REPLACE_OP
                                    + "\"; no other patch operation is supported");
        }

        if (!UpdateOperationalJobRequestPayload.STATUS_PATH.equals(payload.path()))
        {
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST,
                                    "path must be \"" + UpdateOperationalJobRequestPayload.STATUS_PATH
                                    + "\"; no other field of an operational job can be patched");
        }

        if (parsedStatus(payload.value()) != OperationalJobStatus.ABORTED)
        {
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST,
                                    "value must be " + OperationalJobStatus.ABORTED
                                    + "; no other transition is supported");
        }

        boolean force = parseBooleanQueryParam(context.request(), "force", false);
        return new AbortRequest(jobId, force);
    }

    private static OperationalJobStatus parsedStatus(String value)
    {
        if (value == null)
        {
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST, "value is required");
        }
        try
        {
            return OperationalJobStatus.valueOf(value);
        }
        catch (IllegalArgumentException e)
        {
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST, "Unknown job status: " + value);
        }
    }

    @Override
    protected void handleInternal(RoutingContext context,
                                  HttpServerRequest httpRequest,
                                  @NotNull String host,
                                  SocketAddress remoteAddress,
                                  AbortRequest request)
    {
        executorPools.service()
                     .executeBlocking(() -> jobManager.abortJob(request.jobId, request.force))
                     .onSuccess(job -> sendResponse(context, job))
                     .onFailure(cause -> processFailure(cause, context, host, remoteAddress, request));
    }

    @Override
    protected HttpException determineHttpException(Throwable cause)
    {
        if (cause instanceof OperationalJobNotFoundException)
        {
            return wrapHttpException(HttpResponseStatus.NOT_FOUND, cause.getMessage(), cause);
        }

        if (cause instanceof OperationalJobConflictException)
        {
            return wrapHttpException(HttpResponseStatus.CONFLICT, cause.getMessage(), cause);
        }

        if (cause instanceof OperationalJobNotCoordinatedException)
        {
            return wrapHttpException(HttpResponseStatus.BAD_REQUEST, cause.getMessage(), cause);
        }

        if (cause instanceof UnsupportedOperationException)
        {
            return wrapHttpException(HttpResponseStatus.SERVICE_UNAVAILABLE, cause.getMessage(), cause);
        }

        return super.determineHttpException(cause);
    }

    /**
     * Answers {@code 202}: the operation is recorded as aborted, and the cluster still has to wind it down.
     */
    private static void sendResponse(RoutingContext context, OperationalJobInfo job)
    {
        context.response().setStatusCode(HttpResponseStatus.ACCEPTED.code());
        context.json(OperationalJobResponse.builder()
                                           .jobId(job.jobId())
                                           .status(job.status())
                                           .operation(job.name())
                                           .reason(job.failureReason())
                                           .startTime(job.startTime())
                                           .nodesPending(job.nodesPending())
                                           .nodesExecuting(job.nodesExecuting())
                                           .nodesSucceeded(job.nodesSucceeded())
                                           .nodesFailed(job.nodesFailed())
                                           .nodesAborted(job.nodesAborted())
                                           .lastUpdate(job.lastUpdate())
                                           .build());
    }

    static final class AbortRequest
    {
        private final UUID jobId;
        private final boolean force;

        private AbortRequest(UUID jobId, boolean force)
        {
            this.jobId = jobId;
            this.force = force;
        }

        @Override
        public String toString()
        {
            return "AbortRequest{jobId=" + jobId + ", force=" + force + '}';
        }
    }

    private UUID validatedJobId(RoutingContext context)
    {
        String requestJobId = context.pathParam(OPERATIONAL_JOB_ID_PATH_PARAM.substring(1));
        if (requestJobId == null)
        {
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST,
                                    OPERATIONAL_JOB_ID_PATH_PARAM + " is required but not supplied");
        }
        try
        {
            return UUID.fromString(requestJobId);
        }
        catch (IllegalArgumentException e)
        {
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST, "Invalid job ID provided: " + requestJobId);
        }
    }
}
