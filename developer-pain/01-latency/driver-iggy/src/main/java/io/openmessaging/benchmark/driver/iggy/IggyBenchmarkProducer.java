/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.openmessaging.benchmark.driver.iggy;

import io.openmessaging.benchmark.driver.BenchmarkProducer;
import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClient;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.MessageHeader;
import org.apache.iggy.message.MessageId;
import org.apache.iggy.message.Partitioning;

/**
 * Sends each benchmark message as it arrives. The message's origin timestamp carries the publish
 * time in microseconds, which the consumer hands back to the framework for end-to-end latency.
 */
public class IggyBenchmarkProducer implements BenchmarkProducer {

    private final AsyncIggyTcpClient client;
    private final StreamId stream;
    private final TopicId topic;

    public IggyBenchmarkProducer(AsyncIggyTcpClient client, StreamId stream, TopicId topic) {
        this.client = client;
        this.stream = stream;
        this.topic = topic;
    }

    @Override
    public CompletableFuture<Void> sendAsync(Optional<String> key, byte[] payload) {
        long nowMicros = System.currentTimeMillis() * 1000L;
        MessageHeader header =
                new MessageHeader(
                        BigInteger.ZERO,
                        MessageId.serverGenerated(),
                        BigInteger.ZERO,
                        BigInteger.ZERO,
                        BigInteger.valueOf(nowMicros),
                        0L,
                        (long) payload.length,
                        BigInteger.ZERO);
        List<Message> batch = Collections.singletonList(new Message(header, payload, Collections.emptyMap()));
        Partitioning partitioning = key.map(Partitioning::messagesKey).orElseGet(Partitioning::balanced);
        return client.messages().sendMessages(stream, topic, partitioning, batch).thenApply(r -> null);
    }

    @Override
    public void close() {
        client.close().join();
    }
}
