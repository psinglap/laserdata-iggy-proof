import { Consumer, PollingStrategy } from "apache-iggy";
import { connect, ensureTopology, GROUP, type OrderEvent } from "./config.js";

const client = connect();
const { streamId, topicId } = await ensureTopology(client);

// The server owns group membership and partition assignment.
// Start a second copy of this process and the partitions are split between them.
await client.group.ensureAndJoin(streamId, topicId, GROUP);

let running = true;
const stop = () => { running = false; };
process.once("SIGINT", stop);
process.once("SIGTERM", stop);

const handle = (event: OrderEvent, eventType: string) => {
  // Keep this idempotent: delivery is at-least-once, so a restart can repeat a message.
  console.log(`${eventType.padEnd(8)} ${event.orderId} (${event.customerId})`);
};

while (running) {
  const batch = await client.message.poll({
    streamId,
    topicId,
    consumer: Consumer.Group(GROUP),
    partitionId: null, // the server picks this member's partition
    pollingStrategy: PollingStrategy.Next,
    count: 100,
    autocommit: true,
  });

  for (const m of batch.messages) {
    const typeHeader = m.userHeaders.find((h) => h.key.value === "event-type");
    handle(JSON.parse(m.payload.toString()) as OrderEvent, String(typeHeader?.value.value ?? "unknown"));
  }

  if (batch.count === 0) await new Promise((r) => setTimeout(r, 250));
}

// Leave the group so the server reassigns partitions right away, then close the connection.
await client.group.leave({ streamId, topicId, groupId: GROUP });
await client.destroy();
console.log("consumer stopped cleanly");
