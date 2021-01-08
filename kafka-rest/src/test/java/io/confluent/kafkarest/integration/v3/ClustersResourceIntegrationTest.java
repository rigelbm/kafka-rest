/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.kafkarest.integration.v3;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.confluent.kafkarest.entities.v3.ClusterData;
import io.confluent.kafkarest.entities.v3.ClusterDataList;
import io.confluent.kafkarest.entities.v3.GetClusterResponse;
import io.confluent.kafkarest.entities.v3.ListClustersResponse;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.entities.v3.ResourceCollection;
import io.confluent.kafkarest.testing.KafkaClusterEnvironment;
import io.confluent.kafkarest.testing.KafkaRestEnvironment;
import io.confluent.kafkarest.testing.ZookeeperEnvironment;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class ClustersResourceIntegrationTest {

  @Order(1)
  @RegisterExtension
  public static final ZookeeperEnvironment zookeeper = ZookeeperEnvironment.create();

  @Order(2)
  @RegisterExtension
  public static final KafkaClusterEnvironment kafkaCluster =
      KafkaClusterEnvironment.builder()
          .setZookeeper(zookeeper)
          .setNumBrokers(3)
          .build();

  @Order(3)
  @RegisterExtension
  public static final KafkaRestEnvironment kafkaRest =
      KafkaRestEnvironment.builder()
          .setKafkaCluster(kafkaCluster)
          .build();

  @Test
  public void listClusters_returnsArrayWithOwnCluster() {
    String baseUrl = kafkaRest.getBaseUri().toString();
    String clusterId = kafkaCluster.getClusterId();
    int controllerId = kafkaCluster.getControllerID();

    ListClustersResponse expected =
        ListClustersResponse.create(
            ClusterDataList.builder()
                .setMetadata(
                    ResourceCollection.Metadata.builder()
                        .setSelf(baseUrl + "/v3/clusters")
                        .build())
                .setData(
                    singletonList(
                        ClusterData.builder()
                            .setMetadata(
                                Resource.Metadata.builder()
                                    .setSelf(baseUrl + "/v3/clusters/" + clusterId)
                                    .setResourceName("crn:///kafka=" + clusterId)
                                    .build())
                            .setClusterId(clusterId)
                            .setController(
                                Resource.Relationship.create(
                                    baseUrl
                                        + "/v3/clusters/" + clusterId
                                        + "/brokers/" + controllerId))
                            .setAcls(
                                Resource.Relationship.create(
                                    baseUrl + "/v3/clusters/" + clusterId + "/acls"))
                            .setBrokers(
                                Resource.Relationship.create(
                                    baseUrl + "/v3/clusters/" + clusterId + "/brokers"))
                            .setBrokerConfigs(
                                Resource.Relationship.create(
                                    baseUrl + "/v3/clusters/" + clusterId + "/broker-configs"))
                            .setConsumerGroups(
                                Resource.Relationship.create(
                                    baseUrl + "/v3/clusters/" + clusterId + "/consumer-groups"))
                            .setTopics(
                                Resource.Relationship.create(
                                    baseUrl + "/v3/clusters/" + clusterId + "/topics"))
                            .setPartitionReassignments(
                                Resource.Relationship.create(
                                    baseUrl + "/v3/clusters/" + clusterId +
                                        "/topics/-/partitions/-/reassignment"))
                            .build()))
                .build());

    Response response = kafkaRest.request("/v3/clusters").accept(MediaType.APPLICATION_JSON).get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ListClustersResponse actual = response.readEntity(ListClustersResponse.class);
    assertEquals(expected, actual);
  }

  @Test
  public void getCluster_ownCluster_returnsOwnCluster() {
    String baseUrl = kafkaRest.getBaseUri().toString();
    String clusterId = kafkaCluster.getClusterId();
    int controllerId = kafkaCluster.getControllerID();

    GetClusterResponse expected =
        GetClusterResponse.create(
            ClusterData.builder()
                .setMetadata(
                    Resource.Metadata.builder()
                        .setSelf(baseUrl + "/v3/clusters/" + clusterId)
                        .setResourceName("crn:///kafka=" + clusterId)
                        .build())
                .setClusterId(clusterId)
                .setController(
                    Resource.Relationship.create(
                        baseUrl + "/v3/clusters/" + clusterId + "/brokers/" + controllerId))
                .setAcls(
                    Resource.Relationship.create(
                        baseUrl + "/v3/clusters/" + clusterId + "/acls"))
                .setBrokers(
                    Resource.Relationship.create(
                        baseUrl + "/v3/clusters/" + clusterId + "/brokers"))
                .setBrokerConfigs(
                    Resource.Relationship.create(
                        baseUrl + "/v3/clusters/" + clusterId + "/broker-configs"))
                .setConsumerGroups(
                    Resource.Relationship.create(
                        baseUrl + "/v3/clusters/" + clusterId + "/consumer-groups"))
                .setTopics(
                    Resource.Relationship.create(
                        baseUrl + "/v3/clusters/" + clusterId + "/topics"))
                .setPartitionReassignments(
                    Resource.Relationship.create(
                        baseUrl + "/v3/clusters/" + clusterId +
                            "/topics/-/partitions/-/reassignment"))
                .build());

    Response response =
        kafkaRest.request(String.format("/v3/clusters/%s", clusterId))
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    GetClusterResponse actual = response.readEntity(GetClusterResponse.class);
    assertEquals(expected, actual);
  }

  @Test
  public void getCluster_differentCluster_returnsNotFound() {
    Response response =
        kafkaRest.request("/v3/clusters/foobar").accept(MediaType.APPLICATION_JSON).get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }
}
