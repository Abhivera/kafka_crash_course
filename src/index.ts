import express, { Application, Request, Response } from "express";
import "dotenv/config";
import cors from "cors";
import { Kafka } from "kafkajs";  // Import KafkaJS

const app: Application = express();
const PORT = process.env.PORT || 7000;

// * Middleware
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: false }));

// * Kafka Configuration
const kafka = new Kafka({
  clientId: "node-app",  // The client ID for Kafka
  brokers: ["localhost:9092"],  // Kafka broker address (adjust if necessary)
});

// * Producer Setup
const producer = kafka.producer();

// * Consumer Setup
const consumer = kafka.consumer({ groupId: "node-group" });

// Start Kafka Producer and Consumer
const startKafka = async () => {
  // Connect the producer
  await producer.connect();
  console.log("Kafka Producer connected");

  // Connect the consumer
  await consumer.connect();
  await consumer.subscribe({ topic: "my-topic", fromBeginning: true });
  console.log("Kafka Consumer connected and subscribed to 'my-topic'");

  // Consumer message handling
  consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log(`Received message: ${message.value?.toString()}`);
    },
  });
};

startKafka().catch(console.error);

// * Express Route
app.get("/", (req: Request, res: Response) => {
  return res.send("It's working 🙌");
});

// Producer route: Send messages to Kafka
app.post("/send", async (req: Request, res: Response) => {
  const { message } = req.body;

  try {
    await producer.send({
      topic: "my-topic",  // The Kafka topic to which messages will be sent
      messages: [
        {
          value: message,
        },
      ],
    });

    res.send("Message sent to Kafka!");
  } catch (err) {
    console.error("Error sending message to Kafka:", err);
    res.status(500).send("Error sending message to Kafka");
  }
});

app.listen(PORT, () => console.log(`Server is running on PORT ${PORT}`));
