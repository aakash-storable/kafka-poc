const { Kafka, logLevel, Partitioners } = require('kafkajs');

// Kafka Configuration
const kafkaConfig = {
  clientId: 'my-producer',
  brokers: ['localhost:9092'],
  topic: 'test-topic'
};

const kafka = new Kafka({
  clientId: kafkaConfig.clientId,
  brokers: kafkaConfig.brokers,
  logLevel: logLevel.INFO,
  retry: {
    retries: 5,
  }
});

const producer = kafka.producer({
  createPartitioner: Partitioners.LegacyPartitioner,
});

const sendMessage = async () => {
  try {
    await producer.connect();

    const messages = [
      { key: 'key1', value: JSON.stringify({ name: 'John Doe', email: 'john.doe@example.com', age: 30 }) },
      { key: 'key2', value: JSON.stringify({ name: 'Jane Doe', email: 'jane.doe@example.com', age: 28 }) },
    ];

    await producer.send({
      topic: kafkaConfig.topic,
      messages: messages,
    });

    console.log('Messages sent successfully');
  } catch (err) {
    console.error('Failed to send messages', err);
  } finally {
    await producer.disconnect();
  }
};

sendMessage().catch(console.error);
