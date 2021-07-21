const {PubSub} = require('@google-cloud/pubsub');

// create a client
const pubsub = new PubSub();

module.exports = {
  push_msg: async function(msg_str) {
    const topicName = 'dota2-gsi-queue';
    const dataBuffer = Buffer.from(msg_str);
    const messageId = await pubsub.topic(topicName).publish(dataBuffer);
  }
};