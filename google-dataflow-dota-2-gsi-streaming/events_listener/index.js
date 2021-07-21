const d2gsi = require('dota2-gsi');
const server = new d2gsi({port: '80'});
const queue = require('./send');

server.events.on('newclient', client => {
  client.on('newdata', value => {
    // replace unknown character (rare case)
    var json = JSON.stringify(value).replace(/[^\x00-\x7F]/g, '');
    // push json to PubSub queue
    messageId = queue.push_msg('newdata=' + json);
  });
});
