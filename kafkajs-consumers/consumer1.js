const { Kafka, logLevel } = require('kafkajs');

// Kafka Configuration
const kafkaConfig = {
  clientId: 'my-consumer-one',
  brokers: ['localhost:9092'],
  topic: 'test-topic'
};

const kafka = new Kafka({
  clientId: kafkaConfig.clientId,
  brokers: kafkaConfig.brokers,
  logLevel: logLevel.INFO,
});

const consumer = kafka.consumer({ groupId: 'my-group-one' });

const run = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: kafkaConfig.topic, fromBeginning: true });

  console.log('Consumer1 connected and subscribed to topic:', kafkaConfig.topic);

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log(`Received message: ${message.value.toString()}`);
    },
  });
};

run().catch(console.error);
