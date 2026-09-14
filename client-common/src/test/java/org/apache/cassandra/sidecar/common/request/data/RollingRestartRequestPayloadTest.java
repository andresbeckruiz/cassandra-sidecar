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

package org.apache.cassandra.sidecar.common.request.data;

import java.util.Arrays;
import java.util.Collections;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link RollingRestartRequestPayload}
 */
public class RollingRestartRequestPayloadTest
{
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void testDeserializeFullPayload() throws JsonProcessingException
    {
        String json = "{\"datacenter\":\"dc1\","
                      + "\"nodes\":[\"uuid-1\",\"uuid-2\"],"
                      + "\"maxRestartParallelism\":2,"
                      + "\"cassandraHealthTimeoutSeconds\":120,"
                      + "\"nodeStateTransitionTimeoutSeconds\":300,"
                      + "\"nodeRestartRetryAttempts\":3,"
                      + "\"waitBetweenExecutionGroupsSeconds\":90}";

        RollingRestartRequestPayload payload = objectMapper.readValue(json, RollingRestartRequestPayload.class);

        assertThat(payload.datacenter()).isEqualTo("dc1");
        assertThat(payload.nodes()).containsExactly("uuid-1", "uuid-2");
        assertThat(payload.maxRestartParallelism()).isEqualTo(2);
        assertThat(payload.cassandraHealthTimeoutSeconds()).isEqualTo(120);
        assertThat(payload.nodeStateTransitionTimeoutSeconds()).isEqualTo(300);
        assertThat(payload.nodeRestartRetryAttempts()).isEqualTo(3);
        assertThat(payload.waitBetweenExecutionGroupsSeconds()).isEqualTo(90);
    }

    @Test
    void testDeserializeMinimalPayload() throws JsonProcessingException
    {
        String json = "{\"datacenter\":\"dc1\"}";

        RollingRestartRequestPayload payload = objectMapper.readValue(json, RollingRestartRequestPayload.class);

        assertThat(payload.datacenter()).isEqualTo("dc1");
        assertThat(payload.nodes()).isEmpty();
        assertThat(payload.maxRestartParallelism()).isEqualTo(1);
        assertThat(payload.cassandraHealthTimeoutSeconds()).isNull();
        assertThat(payload.nodeStateTransitionTimeoutSeconds()).isNull();
        assertThat(payload.nodeRestartRetryAttempts()).isNull();
        assertThat(payload.waitBetweenExecutionGroupsSeconds()).isNull();
    }

    @Test
    void testNullNodesTreatedAsEmptyList() throws JsonProcessingException
    {
        String json = "{\"datacenter\":\"dc1\",\"nodes\":null}";

        RollingRestartRequestPayload payload = objectMapper.readValue(json, RollingRestartRequestPayload.class);

        assertThat(payload.nodes()).isEmpty();
    }

    @Test
    void testSerialization() throws JsonProcessingException
    {
        RollingRestartRequestPayload payload = new RollingRestartRequestPayload(
            "dc1",
            Arrays.asList("uuid-1", "uuid-2"),
            2,
            120,
            300,
            3,
            90
        );

        String json = objectMapper.writeValueAsString(payload);
        String expectedJson = "{\"datacenter\":\"dc1\","
                              + "\"nodes\":[\"uuid-1\",\"uuid-2\"],"
                              + "\"maxRestartParallelism\":2,"
                              + "\"cassandraHealthTimeoutSeconds\":120,"
                              + "\"nodeStateTransitionTimeoutSeconds\":300,"
                              + "\"nodeRestartRetryAttempts\":3,"
                              + "\"waitBetweenExecutionGroupsSeconds\":90}";
        assertThat(json).isEqualTo(expectedJson);

        RollingRestartRequestPayload deserialized = objectMapper.readValue(json, RollingRestartRequestPayload.class);
        assertThat(deserialized).usingRecursiveComparison().isEqualTo(payload);
    }

    @Test
    void testSerializationOmitsNullFields() throws JsonProcessingException
    {
        RollingRestartRequestPayload payload = new RollingRestartRequestPayload(
            "dc1",
            Collections.emptyList(),
            1,
            null,
            null,
            null,
            null
        );

        String json = objectMapper.writeValueAsString(payload);
        assertThat(json).doesNotContain("cassandraHealthTimeoutSeconds");
        assertThat(json).doesNotContain("nodeStateTransitionTimeoutSeconds");
        assertThat(json).doesNotContain("nodeRestartRetryAttempts");
        assertThat(json).doesNotContain("waitBetweenExecutionGroupsSeconds");
    }
}
