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

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link UpdateOperationalJobRequestPayload}
 */
public class UpdateOperationalJobRequestPayloadTest
{
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void testDeserializeFullPayload() throws JsonProcessingException
    {
        String json = "{\"op\":\"replace\","
                      + "\"path\":\"/status\","
                      + "\"value\":\"ABORTED\"}";

        UpdateOperationalJobRequestPayload payload =
        objectMapper.readValue(json, UpdateOperationalJobRequestPayload.class);

        // Asserted against the constants rather than the literals so that changing either the constants or the
        // wire form on its own fails here; clients are written against the literals.
        assertThat(payload.op()).isEqualTo(UpdateOperationalJobRequestPayload.REPLACE_OP);
        assertThat(payload.path()).isEqualTo(UpdateOperationalJobRequestPayload.STATUS_PATH);
        assertThat(payload.value()).isEqualTo("ABORTED");
    }

    @Test
    void testDeserializeMissingFieldsAreNull() throws JsonProcessingException
    {
        String json = "{\"op\":\"replace\"}";

        UpdateOperationalJobRequestPayload payload =
        objectMapper.readValue(json, UpdateOperationalJobRequestPayload.class);

        // The payload carries whatever arrived; rejecting an incomplete patch is the handler's job.
        assertThat(payload.op()).isEqualTo("replace");
        assertThat(payload.path()).isNull();
        assertThat(payload.value()).isNull();
    }

    @Test
    void testDeserializeUnrootedStatusPath() throws JsonProcessingException
    {
        String json = "{\"op\":\"replace\",\"path\":\"status\",\"value\":\"ABORTED\"}";

        UpdateOperationalJobRequestPayload payload =
        objectMapper.readValue(json, UpdateOperationalJobRequestPayload.class);

        // The unrooted path is not accepted, but it has to survive deserialization unchanged
        // for the handler to reject it.
        assertThat(payload.path()).isEqualTo("status");
    }

    @Test
    void testSerialization() throws JsonProcessingException
    {
        UpdateOperationalJobRequestPayload payload = new UpdateOperationalJobRequestPayload(
            "replace",
            "/status",
            "ABORTED"
        );

        String json = objectMapper.writeValueAsString(payload);
        String expectedJson = "{\"op\":\"replace\","
                              + "\"path\":\"/status\","
                              + "\"value\":\"ABORTED\"}";
        assertThat(json).isEqualTo(expectedJson);

        UpdateOperationalJobRequestPayload deserialized =
        objectMapper.readValue(json, UpdateOperationalJobRequestPayload.class);
        assertThat(deserialized).usingRecursiveComparison().isEqualTo(payload);
    }

    @Test
    void testSerializationOmitsNullFields() throws JsonProcessingException
    {
        UpdateOperationalJobRequestPayload payload = new UpdateOperationalJobRequestPayload(
            "replace",
            null,
            null
        );

        String json = objectMapper.writeValueAsString(payload);
        assertThat(json).doesNotContain("path");
        assertThat(json).doesNotContain("value");
    }

    @Test
    void testToString()
    {
        UpdateOperationalJobRequestPayload payload = new UpdateOperationalJobRequestPayload(
            "replace",
            "/status",
            "ABORTED"
        );

        assertThat(payload.toString())
        .isEqualTo("UpdateOperationalJobRequestPayload{op='replace', path='/status', value='ABORTED'}");
    }
}
