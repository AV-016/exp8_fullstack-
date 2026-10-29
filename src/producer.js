
require('dotenv').config();

const express = require('express');
const { Kafka } = require('kafkajs');
const { randomUUID } = require('node:crypto');

const app = express();
app.use(express.json());

const kafka = new Kafka({
  clientId: 'reliability-producer',
  brokers: [process.env.KAFKA_BROKER]
});

const producer = kafka.producer();

app.post('/events', async (req, res) => {
  try {
    const { message, fail, eventId } = req.body;

    if (typeof message !== 'string' || !message.trim()) {
      return res.status(400).json({
        error: 'message must be a non-empty string'
      });
    }

    const event = {
      eventId: eventId || randomUUID(),
      message,
      fail: fail === true,
      createdAt: new Date().toISOString()
    };

    await producer.send({
      topic: process.env.MAIN_TOPIC,
      messages: [{
        key: event.eventId,
        value: JSON.stringify(event)
      }]
    });

    res.status(202).json({
      status: 'published',
      event
    });
  } catch (error) {
    console.error('Producer error:', error.message);
    res.status(500).json({ error: 'Failed to publish event' });
  }
});

async function start() {
  await producer.connect();

  app.listen(process.env.PORT, () => {
    console.log(
      `Producer API running on http://localhost:${process.env.PORT}`
    );
  });
}

start().catch(error => {
  console.error('Startup failed:', error);
  process.exit(1);
});

async function shutdown() {
  await producer.disconnect();
  process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
