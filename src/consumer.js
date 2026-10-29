
require('dotenv').config();

const { Kafka } = require('kafkajs');
const { isProcessed, markProcessed } = require('./database');

const kafka = new Kafka({
  clientId: 'reliability-consumer',
  brokers: [process.env.KAFKA_BROKER]
});

const consumer = kafka.consumer({
  groupId: 'reliability-group'
});

const producer = kafka.producer();

const maxRetries = Number(process.env.MAX_RETRIES || 3);
const initialDelay = Number(
  process.env.INITIAL_RETRY_DELAY_MS || 1000
);

const sleep = ms =>
  new Promise(resolve => setTimeout(resolve, ms));

async function processEvent(event) {
  console.log(`Processing event: ${event.eventId}`);

  // Simulate a failure for testing retries and the DLQ.
  if (event.fail) {
    throw new Error('Simulated processing failure');
  }

  // Represents the actual business operation.
  console.log(`Business operation completed: ${event.message}`);
}

async function sendToDLQ(event, error) {
  const deadLetter = {
    ...event,
    failedAt: new Date().toISOString(),
    error: error.message,
    originalTopic: process.env.MAIN_TOPIC
  };

  await producer.send({
    topic: process.env.DLQ_TOPIC,
    messages: [{
      key: event.eventId || 'unknown',
      value: JSON.stringify(deadLetter)
    }]
  });

  console.error(`Sent event ${event.eventId} to DLQ`);
}

async function handleEvent(rawValue) {
  let event;

  try {
    event = JSON.parse(rawValue.toString());

    if (
      !event.eventId ||
      typeof event.message !== 'string'
    ) {
      throw new Error('Invalid event structure');
    }
  } catch (error) {
    console.error('Invalid event:', error.message);

    await sendToDLQ(
      event || {
        eventId: `invalid-${Date.now()}`,
        message: rawValue.toString()
      },
      error
    );

    return;
  }

  if (isProcessed(event.eventId)) {
    console.log(`Duplicate ignored: ${event.eventId}`);
    return;
  }

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      await processEvent(event);
      markProcessed(event.eventId);

      console.log(`Successfully processed: ${event.eventId}`);
      return;
    } catch (error) {
      if (attempt === maxRetries) {
        console.error(
          `Retries exhausted for ${event.eventId}`
        );

        await sendToDLQ(event, error);
        return;
      }

      const delay = initialDelay * (2 ** attempt);

      console.warn(
        `Attempt ${attempt + 1} failed: ${error.message}`
      );
      console.log(`Retrying in ${delay} ms`);

      await sleep(delay);
    }
  }
}

async function start() {
  const admin = kafka.admin();

  await admin.connect();

  await admin.createTopics({
    waitForLeaders: true,
    topics: [
      {
        topic: process.env.MAIN_TOPIC,
        numPartitions: 1,
        replicationFactor: 1
      },
      {
        topic: process.env.DLQ_TOPIC,
        numPartitions: 1,
        replicationFactor: 1
      }
    ]
  });

  await admin.disconnect();

  await producer.connect();
  await consumer.connect();

  await consumer.subscribe({
    topic: process.env.MAIN_TOPIC,
    fromBeginning: true
  });

  console.log('Reliability consumer is running');

  await consumer.run({
    eachMessage: async ({ message }) => {
      await handleEvent(message.value);
    }
  });
}

start().catch(error => {
  console.error('Consumer startup failed:', error);
  process.exit(1);
});

async function shutdown() {
  await consumer.disconnect();
  await producer.disconnect();
  process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
