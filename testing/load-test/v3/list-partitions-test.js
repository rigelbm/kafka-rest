import {randomItem, uuidv4} from "https://jslib.k6.io/k6-utils/1.0.0/index.js";

import {createTopic, deleteTopic, listClusters, listPartitions} from './common.js';

export let options = {
    stages: [
        {duration: '10s', target: 100},
        {duration: '40s', target: 100},
        {duration: '10s', target: 0},
    ],
    setupTimeout: '3m'
};

export function setup() {
    let listClustersResponse = listClusters();
    let clusterId = randomItem(listClustersResponse.json().data).cluster_id;

    let topics = [];

    for (let i = 0; i < 100; i++) {
        let topicName = `topic-${uuidv4()}`;
        topics.push(topicName);
        createTopic(clusterId, topicName, 3, 3);
    }

    return {clusterId, topics};
}

export default function (data) {
    listPartitions(data.clusterId, randomItem(data.topics));
}

export function teardown(data) {
    data.topics.forEach(topicName => deleteTopic(data.clusterId, topicName));
}