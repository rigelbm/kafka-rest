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
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.Iterables;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.converters.AvroConverter;
import io.confluent.kafkarest.converters.JsonSchemaConverter;
import io.confluent.kafkarest.converters.ProtobufConverter;
import io.confluent.kafkarest.converters.SchemaConverter;
import io.confluent.kafkarest.converters.SchemaConverter.JsonNodeAndSize;
import io.confluent.kafkarest.entities.v2.ConsumerSubscriptionRecord;
import io.confluent.kafkarest.entities.v2.CreateConsumerInstanceRequest;
import io.confluent.kafkarest.entities.v2.CreateConsumerInstanceResponse;
import io.confluent.kafkarest.entities.v2.SchemaConsumerRecord;
import io.confluent.kafkarest.testing.DefaultKafkaRestTestEnvironment;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.List;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ConsumersResourceIntegrationTest {
  private static final String CONSUMER_GROUP_ID = "group-1";
  private static final String TOPIC_NAME = "topic-1";

  @Rule
  public final DefaultKafkaRestTestEnvironment testEnv = new DefaultKafkaRestTestEnvironment();

  @Before
  public void setUp() throws Exception {
    testEnv.kafkaCluster()
        .createTopic(TOPIC_NAME, /* numPartitions= */ 1, /* replicationFactor= */ (short) 3);
  }

  @Test
  public void consumeBinary_returnsRecords() throws Exception {
    Response createConsumerInstanceResponse =
        testEnv.kafkaRest()
            .target()
            .path(String.format("/consumers/%s", CONSUMER_GROUP_ID))
            .request()
            .post(
                Entity.entity(
                    new CreateConsumerInstanceRequest(
                        /* id= */ null,
                        /* name= */ null,
                        /* format= */ "BINARY",
                        /* autoOffsetReset= */ null,
                        /* autoCommitEnable= */ null,
                        /* responseMinBytes= */ null,
                        /* requestWaitMs= */ null),
                    Versions.KAFKA_V2_JSON));
    assertEquals(Status.OK.getStatusCode(), createConsumerInstanceResponse.getStatus());

    CreateConsumerInstanceResponse createConsumerInstance =
        createConsumerInstanceResponse.readEntity(CreateConsumerInstanceResponse.class);

    Response subscribeResponse =
        testEnv.kafkaRest()
            .target()
            .path(
                String.format(
                    "/consumers/%s/instances/%s/subscription",
                    CONSUMER_GROUP_ID,
                    createConsumerInstance.getInstanceId()))
            .request()
            .post(
                Entity.entity(
                    new ConsumerSubscriptionRecord(
                        singletonList(TOPIC_NAME), null), Versions.KAFKA_V2_JSON));
    assertEquals(Status.NO_CONTENT.getStatusCode(), subscribeResponse.getStatus());

    testEnv.kafkaRest()
        .target()
        .path(
            String.format(
                "/consumers/%s/instances/%s/records",
                CONSUMER_GROUP_ID,
                createConsumerInstance.getInstanceId()))
        .request()
        .accept(Versions.KAFKA_V2_JSON_BINARY)
        .get();

    ConsumerRecord<?, ?> producedRecord =
        testEnv.kafkaCluster()
            .createRecord(
                TOPIC_NAME,
                /* partition= */ 0,
                new ByteArraySerializer(),
                ByteString.copyFromUtf8("foo").toByteArray(),
                new ByteArraySerializer(),
                ByteString.copyFromUtf8("bar").toByteArray(),
                Instant.now(),
                /* headers= */ null);

    Response readRecordsResponse =
        testEnv.kafkaRest()
            .target()
            .path(
                String.format(
                    "/consumers/%s/instances/%s/records",
                    CONSUMER_GROUP_ID,
                    createConsumerInstance.getInstanceId()))
            .request()
            .accept(Versions.KAFKA_V2_JSON_BINARY)
            .get();
    assertEquals(Status.OK.getStatusCode(), readRecordsResponse.getStatus());

    SchemaConsumerRecord readRecord =
        Iterables.getOnlyElement(
            readRecordsResponse.readEntity(new GenericType<List<SchemaConsumerRecord>>() { }));

    assertRecordEquals(
        (data) ->
            new JsonNodeAndSize(
                TextNode.valueOf(base64().encode((byte[]) data)), ((byte[]) data).length),
        producedRecord,
        readRecord);
  }

  @Test
  public void consumeJson_returnsRecords() throws Exception {
    Response createConsumerInstanceResponse =
        testEnv.kafkaRest()
            .target()
            .path(String.format("/consumers/%s", CONSUMER_GROUP_ID))
            .request()
            .post(
                Entity.entity(
                    new CreateConsumerInstanceRequest(
                        /* id= */ null,
                        /* name= */ null,
                        /* format= */ "JSON",
                        /* autoOffsetReset= */ null,
                        /* autoCommitEnable= */ null,
                        /* responseMinBytes= */ null,
                        /* requestWaitMs= */ null),
                    Versions.KAFKA_V2_JSON));
    assertEquals(Status.OK.getStatusCode(), createConsumerInstanceResponse.getStatus());

    CreateConsumerInstanceResponse createConsumerInstance =
        createConsumerInstanceResponse.readEntity(CreateConsumerInstanceResponse.class);

    Response subscribeResponse =
        testEnv.kafkaRest()
            .target()
            .path(
                String.format(
                    "/consumers/%s/instances/%s/subscription",
                    CONSUMER_GROUP_ID,
                    createConsumerInstance.getInstanceId()))
            .request()
            .post(
                Entity.entity(
                    new ConsumerSubscriptionRecord(
                        singletonList(TOPIC_NAME), null), Versions.KAFKA_V2_JSON));
    assertEquals(Status.NO_CONTENT.getStatusCode(), subscribeResponse.getStatus());

    testEnv.kafkaRest()
        .target()
        .path(
            String.format(
                "/consumers/%s/instances/%s/records",
                CONSUMER_GROUP_ID,
                createConsumerInstance.getInstanceId()))
        .request()
        .accept(Versions.KAFKA_V2_JSON_JSON)
        .get();

    KafkaJsonSerializer<JsonNode> serializer = new KafkaJsonSerializer<>();
    serializer.configure(emptyMap(), /* isKey= */ false);
    ConsumerRecord<?, ?> producedRecord =
        testEnv.kafkaCluster()
            .createRecord(
                TOPIC_NAME,
                /* partition= */ 0,
                serializer,
                IntNode.valueOf(1),
                serializer,
                TextNode.valueOf("foobar"),
                Instant.now(),
                /* headers= */ null);

    Response readRecordsResponse =
        testEnv.kafkaRest()
            .target()
            .path(
                String.format(
                    "/consumers/%s/instances/%s/records",
                    CONSUMER_GROUP_ID,
                    createConsumerInstance.getInstanceId()))
            .request()
            .accept(Versions.KAFKA_V2_JSON_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), readRecordsResponse.getStatus());

    SchemaConsumerRecord readRecord =
        Iterables.getOnlyElement(
            readRecordsResponse.readEntity(new GenericType<List<SchemaConsumerRecord>>() { }));

    assertRecordEquals(
        (data) -> new JsonNodeAndSize((JsonNode) data, ((JsonNode) data).size()),
        producedRecord,
        readRecord);
  }

  @Test
  public void consumeAvro_returnsRecords() throws Exception {
    Response createConsumerInstanceResponse =
        testEnv.kafkaRest()
            .target()
            .path(String.format("/consumers/%s", CONSUMER_GROUP_ID))
            .request()
            .post(
                Entity.entity(
                    new CreateConsumerInstanceRequest(
                        /* id= */ null,
                        /* name= */ null,
                        /* format= */ "AVRO",
                        /* autoOffsetReset= */ null,
                        /* autoCommitEnable= */ null,
                        /* responseMinBytes= */ null,
                        /* requestWaitMs= */ null),
                    Versions.KAFKA_V2_JSON));
    assertEquals(Status.OK.getStatusCode(), createConsumerInstanceResponse.getStatus());

    CreateConsumerInstanceResponse createConsumerInstance =
        createConsumerInstanceResponse.readEntity(CreateConsumerInstanceResponse.class);

    Response subscribeResponse =
        testEnv.kafkaRest()
            .target()
            .path(
                String.format(
                    "/consumers/%s/instances/%s/subscription",
                    CONSUMER_GROUP_ID,
                    createConsumerInstance.getInstanceId()))
            .request()
            .post(
                Entity.entity(
                    new ConsumerSubscriptionRecord(
                        singletonList(TOPIC_NAME), null), Versions.KAFKA_V2_JSON));
    assertEquals(Status.NO_CONTENT.getStatusCode(), subscribeResponse.getStatus());

    testEnv.kafkaRest()
        .target()
        .path(
            String.format(
                "/consumers/%s/instances/%s/records",
                CONSUMER_GROUP_ID,
                createConsumerInstance.getInstanceId()))
        .request()
        .accept(Versions.KAFKA_V2_JSON_AVRO)
        .get();

    ConsumerRecord<?, ?> producedRecord =
        testEnv.kafkaCluster()
            .createRecord(
                TOPIC_NAME,
                /* partition= */ 0,
                testEnv.schemaRegistry().createAvroSerializer(/* isKey= */ true),
                1,
                testEnv.schemaRegistry().createAvroSerializer(/* isKey= */ false),
                "foobar",
                Instant.now(),
                /* headers= */ null);

    Response readRecordsResponse =
        testEnv.kafkaRest()
            .target()
            .path(
                String.format(
                    "/consumers/%s/instances/%s/records",
                    CONSUMER_GROUP_ID,
                    createConsumerInstance.getInstanceId()))
            .request()
            .accept(Versions.KAFKA_V2_JSON_AVRO)
            .get();
    assertEquals(Status.OK.getStatusCode(), readRecordsResponse.getStatus());

    SchemaConsumerRecord readRecord =
        Iterables.getOnlyElement(
            readRecordsResponse.readEntity(new GenericType<List<SchemaConsumerRecord>>() { }));

    assertRecordEquals(new AvroConverter(), producedRecord, readRecord);
  }

  @Test
  public void consumeJsonschema_returnsRecords() throws Exception {
    Response createConsumerInstanceResponse =
        testEnv.kafkaRest()
            .target()
            .path(String.format("/consumers/%s", CONSUMER_GROUP_ID))
            .request()
            .post(
                Entity.entity(
                    new CreateConsumerInstanceRequest(
                        /* id= */ null,
                        /* name= */ null,
                        /* format= */ "JSONSCHEMA",
                        /* autoOffsetReset= */ null,
                        /* autoCommitEnable= */ null,
                        /* responseMinBytes= */ null,
                        /* requestWaitMs= */ null),
                    Versions.KAFKA_V2_JSON));
    assertEquals(Status.OK.getStatusCode(), createConsumerInstanceResponse.getStatus());

    CreateConsumerInstanceResponse createConsumerInstance =
        createConsumerInstanceResponse.readEntity(CreateConsumerInstanceResponse.class);

    Response subscribeResponse =
        testEnv.kafkaRest()
            .target()
            .path(
                String.format(
                    "/consumers/%s/instances/%s/subscription",
                    CONSUMER_GROUP_ID,
                    createConsumerInstance.getInstanceId()))
            .request()
            .post(
                Entity.entity(
                    new ConsumerSubscriptionRecord(
                        singletonList(TOPIC_NAME), null), Versions.KAFKA_V2_JSON));
    assertEquals(Status.NO_CONTENT.getStatusCode(), subscribeResponse.getStatus());

    testEnv.kafkaRest()
        .target()
        .path(
            String.format(
                "/consumers/%s/instances/%s/records",
                CONSUMER_GROUP_ID,
                createConsumerInstance.getInstanceId()))
        .request()
        .accept(Versions.KAFKA_V2_JSON_JSON_SCHEMA)
        .get();

    ConsumerRecord<?, ?> producedRecord =
        testEnv.kafkaCluster()
            .createRecord(
                TOPIC_NAME,
                /* partition= */ 0,
                testEnv.schemaRegistry().createJsonSchemaSerializer(/* isKey= */ true),
                1,
                testEnv.schemaRegistry().createJsonSchemaSerializer(/* isKey= */ false),
                "foobar",
                Instant.now(),
                /* headers= */ null);

    Response readRecordsResponse =
        testEnv.kafkaRest()
            .target()
            .path(
                String.format(
                    "/consumers/%s/instances/%s/records",
                    CONSUMER_GROUP_ID,
                    createConsumerInstance.getInstanceId()))
            .request()
            .accept(Versions.KAFKA_V2_JSON_JSON_SCHEMA)
            .get();
    assertEquals(Status.OK.getStatusCode(), readRecordsResponse.getStatus());

    SchemaConsumerRecord readRecord =
        Iterables.getOnlyElement(
            readRecordsResponse.readEntity(new GenericType<List<SchemaConsumerRecord>>() { }));

    assertRecordEquals(new JsonSchemaConverter(), producedRecord, readRecord);
  }

  @Test
  public void consumeProtobuf_returnsRecords() throws Exception {
    Response createConsumerInstanceResponse =
        testEnv.kafkaRest()
            .target()
            .path(String.format("/consumers/%s", CONSUMER_GROUP_ID))
            .request()
            .post(
                Entity.entity(
                    new CreateConsumerInstanceRequest(
                        /* id= */ null,
                        /* name= */ null,
                        /* format= */ "PROTOBUF",
                        /* autoOffsetReset= */ null,
                        /* autoCommitEnable= */ null,
                        /* responseMinBytes= */ null,
                        /* requestWaitMs= */ null),
                    Versions.KAFKA_V2_JSON));
    assertEquals(Status.OK.getStatusCode(), createConsumerInstanceResponse.getStatus());

    CreateConsumerInstanceResponse createConsumerInstance =
        createConsumerInstanceResponse.readEntity(CreateConsumerInstanceResponse.class);

    Response subscribeResponse =
        testEnv.kafkaRest()
            .target()
            .path(
                String.format(
                    "/consumers/%s/instances/%s/subscription",
                    CONSUMER_GROUP_ID,
                    createConsumerInstance.getInstanceId()))
            .request()
            .post(
                Entity.entity(
                    new ConsumerSubscriptionRecord(
                        singletonList(TOPIC_NAME), null), Versions.KAFKA_V2_JSON));
    assertEquals(Status.NO_CONTENT.getStatusCode(), subscribeResponse.getStatus());

    testEnv.kafkaRest()
        .target()
        .path(
            String.format(
                "/consumers/%s/instances/%s/records",
                CONSUMER_GROUP_ID,
                createConsumerInstance.getInstanceId()))
        .request()
        .accept(Versions.KAFKA_V2_JSON_PROTOBUF)
        .get();

    ConsumerRecord<?, ?> producedRecord =
        testEnv.kafkaCluster()
            .createRecord(
                TOPIC_NAME,
                /* partition= */ 0,
                testEnv.schemaRegistry().createProtobufSerializer(/* isKey= */ true),
                createMessage(
                    new ProtobufSchema("syntax = \"proto3\"; message Key { int32 key = 1; }"),
                    "key",
                    1),
                testEnv.schemaRegistry().createProtobufSerializer(/* isKey= */ false),
                createMessage(
                    new ProtobufSchema("syntax = \"proto3\"; message Value { string value = 1; }"),
                    "value",
                    "foobar"),
                Instant.now(),
                /* headers= */ null);

    Response readRecordsResponse =
        testEnv.kafkaRest()
            .target()
            .path(
                String.format(
                    "/consumers/%s/instances/%s/records",
                    CONSUMER_GROUP_ID,
                    createConsumerInstance.getInstanceId()))
            .request()
            .accept(Versions.KAFKA_V2_JSON_PROTOBUF)
            .get();
    assertEquals(Status.OK.getStatusCode(), readRecordsResponse.getStatus());

    SchemaConsumerRecord readRecord =
        Iterables.getOnlyElement(
            readRecordsResponse.readEntity(new GenericType<List<SchemaConsumerRecord>>() { }));

    assertRecordEquals(new ProtobufConverter(), producedRecord, readRecord);
  }

  @Test
  public void seekToTimestampThenConsume_returnsRecordsAfterTimestamp() throws Exception {
    Response createConsumerInstanceResponse =
        testEnv.kafkaRest()
            .target()
            .path(String.format("/consumers/%s", CONSUMER_GROUP_ID))
            .request()
            .post(
                Entity.entity(
                    new CreateConsumerInstanceRequest(
                        /* id= */ null,
                        /* name= */ null,
                        /* format= */ "JSON",
                        /* autoOffsetReset= */ null,
                        /* autoCommitEnable= */ null,
                        /* responseMinBytes= */ null,
                        /* requestWaitMs= */ null),
                    Versions.KAFKA_V2_JSON));
    assertEquals(Status.OK.getStatusCode(), createConsumerInstanceResponse.getStatus());

    CreateConsumerInstanceResponse createConsumerInstance =
        createConsumerInstanceResponse.readEntity(CreateConsumerInstanceResponse.class);

    Response subscribeResponse =
        testEnv.kafkaRest()
            .target()
            .path(
                String.format(
                    "/consumers/%s/instances/%s/subscription",
                    CONSUMER_GROUP_ID,
                    createConsumerInstance.getInstanceId()))
            .request()
            .post(
                Entity.entity(
                    new ConsumerSubscriptionRecord(
                        singletonList(TOPIC_NAME), null), Versions.KAFKA_V2_JSON));
    assertEquals(Status.NO_CONTENT.getStatusCode(), subscribeResponse.getStatus());

    testEnv.kafkaRest()
        .target()
        .path(
            String.format(
                "/consumers/%s/instances/%s/records",
                CONSUMER_GROUP_ID,
                createConsumerInstance.getInstanceId()))
        .request()
        .accept(Versions.KAFKA_V2_JSON_JSON)
        .get();

    KafkaJsonSerializer<JsonNode> serializer = new KafkaJsonSerializer<>();
    serializer.configure(emptyMap(), /* isKey= */ false);
    testEnv.kafkaCluster()
        .createRecord(
            TOPIC_NAME,
            /* partition= */ 0,
            serializer,
            IntNode.valueOf(1),
            serializer,
            TextNode.valueOf("foobar"),
            Instant.ofEpochMilli(1000),
            /* headers= */ null);
    ConsumerRecord<?, ?> producedRecordAfter =
        testEnv.kafkaCluster()
            .createRecord(
                TOPIC_NAME,
                /* partition= */ 0,
                serializer,
                IntNode.valueOf(1),
                serializer,
                TextNode.valueOf("foobar"),
                Instant.ofEpochMilli(3000),
                /* headers= */ null);

    Response seekResponse =
        testEnv.kafkaRest()
            .target()
            .path(
                String.format(
                    "/consumers/%s/instances/%s/positions",
                    CONSUMER_GROUP_ID,
                    createConsumerInstance.getInstanceId()))
            .request()
            .post(
                Entity.entity(
                    String.format(
                        "{\"timestamps\":"
                            +"[{\"topic\": \"%s\", \"partition\": %d, \"timestamp\": \"%s\"}]}",
                        TOPIC_NAME,
                        0,
                        DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochMilli(2000))),
                    Versions.KAFKA_V2_JSON));
    assertEquals(Status.NO_CONTENT.getStatusCode(), seekResponse.getStatus());

    Response readRecordsResponse =
        testEnv.kafkaRest()
            .target()
            .path(
                String.format(
                    "/consumers/%s/instances/%s/records",
                    CONSUMER_GROUP_ID,
                    createConsumerInstance.getInstanceId()))
            .request()
            .accept(Versions.KAFKA_V2_JSON_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), readRecordsResponse.getStatus());

    SchemaConsumerRecord readRecord =
        Iterables.getOnlyElement(
            readRecordsResponse.readEntity(new GenericType<List<SchemaConsumerRecord>>() { }));

    assertRecordEquals(
        (data) -> new JsonNodeAndSize((JsonNode) data, ((JsonNode) data).size()),
        producedRecordAfter,
        readRecord);
  }

  private static void assertRecordEquals(
      SchemaConverter schemaConverter, ConsumerRecord<?, ?> expected, SchemaConsumerRecord actual) {
    assertEquals(expected.topic(), actual.getTopic());
    assertEquals(Integer.valueOf(expected.partition()), actual.getPartition());
    assertEquals(Long.valueOf(expected.offset()), actual.getOffset());
    assertEquals(schemaConverter.toJson(expected.key()).getJson(), actual.getKey());
    assertEquals(schemaConverter.toJson(expected.value()).getJson(), actual.getValue());
  }

  private static DynamicMessage createMessage(
      ProtobufSchema schema, String fieldName, Object value) {
    DynamicMessage.Builder messageBuilder = schema.newMessageBuilder();
    FieldDescriptor field = messageBuilder.getDescriptorForType().findFieldByName(fieldName);
    messageBuilder.setField(field, value);
    return messageBuilder.build();
  }
}
