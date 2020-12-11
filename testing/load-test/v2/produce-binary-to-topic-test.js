import encoding from "k6/encoding";

import {randomIntBetween, randomItem, uuidv4} from "https://jslib.k6.io/k6-utils/1.0.0/index.js";

import {produceBinaryToTopic, randomByteString} from "./common.js";
import {createTopic} from "../v3/common.js";

export let options = {
    stages: [
        {duration: '60s', target: 1000},
        {duration: '3480s', target: 1000},
        {duration: '60s', target: 0},
    ],
    setupTimeout: '1m',
    teardownTimeout: '1m'
};

export function setup() {
    let topics = [];
    for (let i = 0; i < 0; i++) {
        let topicName = `topic-binary-${uuidv4()}`;
        topics.push(topicName);
        createTopic(clusterId, topicName, 3, 3);
    }

    let keys = [];
    for (let i = 0; i < 1000; i++) {
        keys.push(encoding.b64encode(randomByteString(1024)));
    }

    return {topics, keys, message: encoding.b64encode(randomByteString(1024))};
}

export default function (data) {
    let records = [];
    for (let i = 0; i < 1; i++) {
        records.push({
            key: randomItem(data.keys),
            value: data.message
        });
    }

    produceBinaryToTopic('rest-proxy-benchmark', records);
}

export function teardown(data) {
    // Don't delete topics. Use the messages produced on consumer-binary-test.
}
