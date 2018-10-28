'use strict';

const request = require('simple-server-request');

function getMessages (url, offset, limit, cb) {
    request.get(url, {offset, limit}, cb);
}

function MessagesConsumer (queueUrl, topic, startingOffset, messagesCountPerRequest) {
    this.currOffset = startingOffset;
    this.count = messagesCountPerRequest || 1;
    this.topic = topic;
    this.topicUrl = `${queueUrl}/${this.topic}`;

    this.start = function (eventsHandler) {
        const Consumer = this;

        function getMessagesCb (err, events) {
            if (err) {
                console.error('Got error while trying to consume messages - ' + err);
                eventsHandler(err);
            }
            else if (events.length === 0) {
                // No messages waiting, will try to fetch new messages in 200 mills
                setTimeout(function () {getMessages(Consumer.topicUrl, Consumer.currOffset, Consumer.count, getMessagesCb)}, 200);
            }
            else {
                // There are messages in the response, will update offset for next request, will call the handler and will go fetch new messages
                Consumer.currOffset += events.length;
                eventsHandler(null, events);
                // TODO: Check if we need to sync the next message consumption with the event handler operation.
                // TODO: We can add a cb that will be called at the end of the event handling)

                setImmediate(function () {getMessages(Consumer.topicUrl, Consumer.currOffset, Consumer.count, getMessagesCb)});
            }
        }

        getMessages(this.topicUrl, this.currOffset, 1, getMessagesCb);
    }
}

module.exports = MessagesConsumer;
//
// const Consumer = new MessagesConsumer('url', 'topic', 0, 3);
//
// Consumer.start(function (err, message) {
//     console.log(JSON.stringify({err, message}));
// });