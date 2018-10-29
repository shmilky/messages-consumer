'use strict';

const request = require('simple-server-request');

function getMessages (url, offset, limit, cb) {
    request.get(url, {offset, limit}, cb);
}

/**
 * Will read messages iteratively from queueUrl/topic according to defined params and will call the events handler with the results (will not stop on error)
 * @param queueUrl - full queue url
 * @param topic - the topic to read messages from
 * @param options - define optional parameters to the messages consumer (startingOffset, messagesCountPerRequest, waitingForNewMessagesTimeout)
 */
function MessagesConsumer (queueUrl, topic, options={}) {
    this.currOffset = options.startingOffset || 0;
    this.count = options.messagesCountPerRequest || 1;
    this.topic = topic;
    this.topicUrl = `${queueUrl}/${this.topic}`;
    this.waitingForNewMessagesTimeout = options.waitingForNewMessagesTimeout;

    this.start = function (eventsHandler) {
        const Consumer = this;

        function getMessagesCb (err, events) {
            if (err) {
                console.error('Got error while trying to consume messages - ' + err);
                eventsHandler(err);
            }

            if (err || events.length === 0) {
                // No messages waiting, will try to fetch new messages in 200 mills
                setTimeout(function () {getMessages(Consumer.topicUrl, Consumer.currOffset, Consumer.count, getMessagesCb)}, Consumer.waitingForNewMessagesTimeout);
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