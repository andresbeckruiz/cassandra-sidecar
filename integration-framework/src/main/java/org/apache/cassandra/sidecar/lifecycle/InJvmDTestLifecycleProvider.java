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

package org.apache.cassandra.sidecar.lifecycle;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;

/**
 * Manages the lifecycle of JVM Dtest Cassandra instances.
 * This should be used for integration tests where Cassandra instances are started and stopped
 */
public class InJvmDTestLifecycleProvider implements LifecycleProvider
{
    private final Iterable<? extends IInstance> instances;
    // Resolved hosts are remembered because an instance can only report its broadcast address while it is running
    private final Map<String, IInstance> instancesByHost = new ConcurrentHashMap<>();

    public InJvmDTestLifecycleProvider(Iterable<? extends IInstance> instances)
    {
        this.instances = instances;
    }

    @Override
    public void start(InstanceMetadata instanceMetadata)
    {
        getInstance(instanceMetadata).startup();
    }

    @Override
    public void stop(InstanceMetadata instanceMetadata)
    {
        try
        {
            // Synchronous to ensure JMX port is released before start again
            getInstance(instanceMetadata).shutdown().get(1, TimeUnit.MINUTES);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isRunning(InstanceMetadata host)
    {
        return !getInstance(host).isShutdown();
    }

    /**
     * Resolves the instance for {@code instanceMetadata}, remembering each host it resolves.
     *
     * <p>On some Cassandra versions {@link IInstance#broadcastAddress()} is routed through the dtest delegate,
     * which is discarded when an instance shuts down, so scanning throws once any instance is stopped. That would
     * leave a stopped instance unresolvable and therefore impossible to start again, so the remembered hosts are
     * used to answer when the scan cannot. An instance is always resolved while it is still up — that is how it
     * got stopped — so it is remembered by the time starting it needs it.</p>
     */
    private IInstance getInstance(InstanceMetadata instanceMetadata)
    {
        try
        {
            for (IInstance instance : instances)
            {
                if (instance.broadcastAddress().getHostName().equals(instanceMetadata.host()))
                {
                    instancesByHost.put(instanceMetadata.host(), instance);
                    return instance;
                }
            }
        }
        catch (IllegalStateException e)
        {
            IInstance known = instancesByHost.get(instanceMetadata.host());
            if (known != null)
            {
                return known;
            }
            throw e;
        }
        throw new IllegalArgumentException("No instance found for host: " + instanceMetadata);
    }
}
