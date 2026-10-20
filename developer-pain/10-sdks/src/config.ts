import { Client } from "apache-iggy";

// One place for names and the connection, so producer and consumer agree.
export const STREAM = "shop";
export const TOPIC = "orders";
export const PARTITIONS = 4;
export const GROUP = "billing";

export const CONNECTION =
  process.env.IGGY_CONNECTION_STRING ?? "iggy+tcp://iggy:iggy@127.0.0.1:8090";

export type OrderEvent = {
  orderId: string;
  customerId: string;
  type: "created" | "paid" | "shipped";
  amountCents: number;
};

export function connect(): Client {
  return new Client(CONNECTION);
}

// Create the stream and topic if they don't exist yet. Safe to call on every start.
export async function ensureTopology(client: Client) {
  const stream = await client.stream.ensure(STREAM);
  const topic = await client.topic.ensure(stream.id, TOPIC, PARTITIONS);
  return { streamId: stream.id, topicId: topic.id };
}
