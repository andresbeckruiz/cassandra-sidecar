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

package org.apache.cassandra.sidecar.job;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

import org.jetbrains.annotations.NotNull;

/**
 * Tracks and stores the results of long-running jobs running on the sidecar.
 * Implementations can use different storage backends (in-memory, persistent, etc.)
 */
public interface OperationalJobTracker
{
    /**
     * Retrieve a job by its ID, or compute and store it if absent.
     * <p>
     * The mapping function is only called if the job does not exist.
     * This ensures that job creation and scheduling logic is only executed once.
     *
     * @param jobId the job identifier
     * @param mappingFunction function to create the job if absent
     * @return the job (either existing or newly created)
     */
    OperationalJob computeIfAbsent(UUID jobId, Function<UUID, OperationalJob> mappingFunction);

    /**
     * Retrieve a job by its ID.
     *
     * @param jobId the job identifier
     * @return the job info, or null if not found
     */
    OperationalJobInfo get(UUID jobId);

    /**
     * Returns an immutable view of all tracked jobs.
     * <p>
     * This provides a consistent snapshot of the jobs being tracked,
     * minimizing contention with concurrent operations.
     *
     * @return an immutable map of job IDs to jobs
     */
    @NotNull
    Map<UUID, OperationalJob> jobsView();

    /**
     * Filters inflight (CREATED or RUNNING) jobs matching the operation name.
     *
     * @param operation the operation name to filter by
     * @return list of inflight jobs for the operation
     */
    @NotNull
    List<OperationalJob> inflightJobsByOperation(String operation);

    /**
     * Returns a snapshot of all inflight (CREATED or RUNNING) jobs tracked by this Sidecar.
     * <p>
     * Unlike {@link #jobsView()}, which exposes only the in-process live map, durable 
     * implementations also merge in still-active jobs reconstructed from storage. This surfaces
     * coordinated jobs (e.g. rolling restart) that are evicted from the in-process map almost
     * immediately after handoff but are long running. The storage-backed portion is scoped 
     * to the local datacenter.
     *
     * @return list of inflight job info, never null
     */
    @NotNull
    List<OperationalJobInfo> inflightJobs();

    /**
     * Records a coordinated cluster-wide operation as aborted at an operator's request. Only updates the 
     * stored job record.
     * <p>
     * Under {@code force} the node job rows in storage of the Sidecars that have not settled their own
     * are written too.
     * <p>
     * An operation that finished on its own cannot be aborted, so it keeps the outcome it recorded. An operation
     * already aborted can, which is what makes a repeated request converge.
     * <p>
     * Only a tracker that stores job state durably can abort, because the abort is a durable transition that the
     * Sidecars running the operation observe from storage. Implementations that track jobs in-process only throw
     * {@link UnsupportedOperationException}.
     * <p>
     * Performs blocking I/O, so callers must run it off the event loop.
     *
     * @param jobId the operation to abort
     * @param force whether to settle the node rows that the Sidecars owning them have not settled
     * @return the job as it stands once aborted
     * @throws UnsupportedOperationException if this tracker does not store job state durably
     */
    OperationalJobInfo markJobAsAborted(UUID jobId, boolean force);
}
