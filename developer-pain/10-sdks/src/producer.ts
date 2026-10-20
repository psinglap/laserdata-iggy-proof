import { HeaderKeyFactory, HeaderValue, Partitioning } from "apache-iggy";
import { connect, ensureTopology, type OrderEvent } from "./config.js";

const client = connect();
const { streamId, topicId } = await ensureTopology(client);

const steps: OrderEvent["type"][] = ["created", "paid", "shipped"];
const customers = ["c-101", "c-102", "c-103"];

for (let i = 1; i <= 30; i++) {
  const customerId = customers[i % customers.length];
  const event: OrderEvent = {
    orderId: `o-${Math.ceil(i / 3)}-${customerId}`,
    customerId,
    type: steps[(i - 1) % steps.length],
    amountCents: 1999,
  };

  await client.message.send({
    streamId,
    topicId,
    // Same key, same partition: every event for one customer stays in order.
    partition: Partitioning.MessageKey(customerId),
    messages: [
      {
        payload: JSON.stringify(event),
        // Metadata goes in headers, so consumers can route without parsing the payload.
        headers: [
          { key: HeaderKeyFactory.String("event-type"), value: HeaderValue.String(event.type) },
        ],
      },
    ],
  });
  console.log(`sent ${event.type} for ${customerId}`);
}

await client.destroy();
