/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafkarest.integration.v2;

import static com.google.common.io.BaseEncoding.base64;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.Iterables;
import com.google.protobuf.ByteString;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.converters.AvroConverter;
import io.confluent.kafkarest.converters.JsonSchemaConverter;
import io.confluent.kafkarest.converters.ProtobufConverter;
import io.confluent.kafkarest.converters.SchemaConverter;
import io.confluent.kafkarest.converters.SchemaConverter.JsonNodeAndSize;
import io.confluent.kafkarest.entities.v2.ProduceRequest;
import io.confluent.kafkarest.entities.v2.ProduceRequest.ProduceRecord;
import io.confluent.kafkarest.entities.v2.ProduceResponse;
import io.confluent.kafkarest.testing.DefaultKafkaRestTestEnvironment;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response.Status;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ProduceActionIntegrationTest {
  private static final String TOPIC_NAME = "topic-1";

  @Rule
  public final DefaultKafkaRestTestEnvironment testEnv = new DefaultKafkaRestTestEnvironment();

  @Before
  public void setUp() throws Exception {
    testEnv.kafkaCluster()
        .createTopic(TOPIC_NAME, /* numPartitions= */ 1, /* replicationFactor= */ (short) 3);
  }

  @Test
  public void produceBinaryToTopic_producesRecord() {
    ProduceRecord produceRecord =
        ProduceRecord.create(
            TextNode.valueOf(base64().encode(ByteString.copyFromUtf8("foo").toByteArray())),
            TextNode.valueOf(base64().encode(ByteString.copyFromUtf8("bar").toByteArray())));
    ProduceRequest produceRequest =
        ProduceRequest.create(singletonList(produceRecord));

    ProduceResponse produceResponse =
        testEnv.kafkaRest()
            .target()
            .path(String.format("/topics/%s", TOPIC_NAME))
            .request()
            .post(Entity.entity(produceRequest, Versions.KAFKA_V2_JSON_BINARY))
            .readEntity(ProduceResponse.class);
    assertEquals(Status.OK, produceResponse.getRequestStatus());

    ConsumerRecord<?, ?> producedRecord =
        testEnv.kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                /* partition= */ 0,
                Iterables.getOnlyElement(produceResponse.getOffsets()).getOffset(),
                new ByteArrayDeserializer(),
                new ByteArrayDeserializer());

    assertRecordEquals(
        (data) ->
            new JsonNodeAndSize(
                TextNode.valueOf(base64().encode((byte[]) data)), ((byte[]) data).length),
        produceRecord,
        producedRecord);
  }

  @Test
  public void produceJsonToTopic_producesRecord() {
    ProduceRecord produceRecord =
        ProduceRecord.create(IntNode.valueOf(1), TextNode.valueOf("foobar"));
    ProduceRequest produceRequest =
        ProduceRequest.create(singletonList(produceRecord));

    ProduceResponse produceResponse =
        testEnv.kafkaRest()
            .target()
            .path(String.format("/topics/%s", TOPIC_NAME))
            .request()
            .post(Entity.entity(produceRequest, Versions.KAFKA_V2_JSON_JSON))
            .readEntity(ProduceResponse.class);
    assertEquals(Status.OK, produceResponse.getRequestStatus());

    KafkaJsonDeserializer<JsonNode> deserializer = new KafkaJsonDeserializer<>();
    deserializer.configure(emptyMap(), /* isKey= */ false);
    ConsumerRecord<JsonNode, JsonNode> producedRecord =
        testEnv.kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                /* partition= */ 0,
                Iterables.getOnlyElement(produceResponse.getOffsets()).getOffset(),
                deserializer,
                deserializer);

    assertRecordEquals(
        (data) ->
            new JsonNodeAndSize(
                new ObjectMapper().valueToTree(data), new ObjectMapper().valueToTree(data).size()),
        produceRecord,
        producedRecord);
  }

  @Test
  public void produceAvroToTopic_producesRecord() {
    ProduceRecord produceRecord =
        ProduceRecord.create(IntNode.valueOf(1), TextNode.valueOf("foobar"));
    ProduceRequest produceRequest =
        ProduceRequest.create(
            singletonList(produceRecord),
            null,
            "{\"type\":\"int\"}",
            null,
            "{\"type\":\"string\"}");

    ProduceResponse produceResponse =
        testEnv.kafkaRest()
            .target()
            .path(String.format("/topics/%s", TOPIC_NAME))
            .request()
            .post(Entity.entity(produceRequest, Versions.KAFKA_V2_JSON_AVRO))
            .readEntity(ProduceResponse.class);
    assertEquals(Status.OK, produceResponse.getRequestStatus());

    ConsumerRecord<?, ?> producedRecord =
        testEnv.kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                /* partition= */ 0,
                Iterables.getOnlyElement(produceResponse.getOffsets()).getOffset(),
                testEnv.schemaRegistry().createAvroDeserializer(),
                testEnv.schemaRegistry().createAvroDeserializer());

    assertRecordEquals(new AvroConverter(), produceRecord, producedRecord);
  }

  @Test
  public void produceJsonschemaToTopic_producesRecord() {
    ProduceRecord produceRecord =
        ProduceRecord.create(IntNode.valueOf(1), TextNode.valueOf("foobar"));
    ProduceRequest produceRequest =
        ProduceRequest.create(
            singletonList(produceRecord),
            null,
            "{\"type\":\"number\"}",
            null,
            "{\"type\":\"string\"}");

    ProduceResponse produceResponse =
        testEnv.kafkaRest()
            .target()
            .path(String.format("/topics/%s", TOPIC_NAME))
            .request()
            .post(Entity.entity(produceRequest, Versions.KAFKA_V2_JSON_JSON_SCHEMA))
            .readEntity(ProduceResponse.class);
    assertEquals(Status.OK, produceResponse.getRequestStatus());

    ConsumerRecord<?, ?> producedRecord =
        testEnv.kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                /* partition= */ 0,
                Iterables.getOnlyElement(produceResponse.getOffsets()).getOffset(),
                testEnv.schemaRegistry().createJsonSchemaDeserializer(),
                testEnv.schemaRegistry().createJsonSchemaDeserializer());

    assertRecordEquals(new JsonSchemaConverter(), produceRecord, producedRecord);
  }

  @Test
  public void produceProtobufToTopic_producesRecord() {
    ProduceRecord produceRecord =
        ProduceRecord.create(
            createObjectNode("key", IntNode.valueOf(1)),
            createObjectNode("value", TextNode.valueOf("foobar")));
    ProduceRequest produceRequest =
        ProduceRequest.create(
            singletonList(produceRecord),
            null,
            "syntax = \"proto3\"; message Key { int32 key = 1; }",
            null,
            "syntax = \"proto3\"; message Value { string value = 1; }");

    ProduceResponse produceResponse =
        testEnv.kafkaRest()
            .target()
            .path(String.format("/topics/%s", TOPIC_NAME))
            .request()
            .post(Entity.entity(produceRequest, Versions.KAFKA_V2_JSON_PROTOBUF))
            .readEntity(ProduceResponse.class);
    assertEquals(Status.OK, produceResponse.getRequestStatus());

    ConsumerRecord<?, ?> producedRecord =
        testEnv.kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                /* partition= */ 0,
                Iterables.getOnlyElement(produceResponse.getOffsets()).getOffset(),
                testEnv.schemaRegistry().createProtobufDeserializer(),
                testEnv.schemaRegistry().createProtobufDeserializer());

    assertRecordEquals(new ProtobufConverter(), produceRecord, producedRecord);
  }

  private static void assertRecordEquals(
      SchemaConverter schemaConverter, ProduceRecord expected, ConsumerRecord<?, ?> actual) {
    assertEquals(expected.getKey().get(), schemaConverter.toJson(actual.key()).getJson());
    assertEquals(expected.getValue().get(), schemaConverter.toJson(actual.value()).getJson());
  }

  private static ObjectNode createObjectNode(String field, JsonNode value) {
    ObjectNode node = new ObjectNode(JsonNodeFactory.instance);
    node.set(field, value);
    return node;
  }
}
