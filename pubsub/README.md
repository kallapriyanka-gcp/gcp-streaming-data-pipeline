# Google Cloud Pub/Sub

## What is Pub/Sub?

Google Cloud Pub/Sub is a messaging service used to send and receive messages between applications and data processing systems.

It helps build event-driven and streaming data pipelines.

## Key Components

- Publisher – sends messages
- Topic – receives and stores messages
- Subscription – receives messages from a topic
- Subscriber – reads messages from a subscription
- Message – the data being sent

## Hands-on

Created a Pub/Sub topic in Google Cloud Console.

### Topic

`sales-data-topic`

### Subscription

A default subscription was created along with the topic.

## Basic Flow

Publisher
↓
Pub/Sub Topic
↓
Subscription
↓
Subscriber / Data Processing


## Hands-on Message Testing

Published a test JSON message to the `sales-data-topic` topic.

### Test Message

```json
{
  "order_id": 1001,
  "customer": "Priyanka",
  "amount": 500
}