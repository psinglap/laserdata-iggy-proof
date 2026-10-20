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

import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClient;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.identifier.ConsumerId;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;

/**
 * Polls the next messages for its consumer group in a loop on a dedicated thread, with
 * auto-commit, and reports each message's origin timestamp (converted to milliseconds) as the
 * publish time.
 */
public class IggyBenchmarkConsumer implements BenchmarkConsumer {

    private final AsyncIggyTcpClient client;
    private final ExecutorService loop = Executors.newSingleThreadExecutor();
    private volatile boolean running = true;

    public IggyBenchmarkConsumer(
            AsyncIggyTcpClient client,
            StreamId stream,
            TopicId topic,
            String group,
            IggyConfig config,
            ConsumerCallback callback) {
        this.client = client;
        Consumer consumer = Consumer.group(ConsumerId.of(group));
        loop.submit(
                () -> {
                    while (running) {
                        try {
                            PolledMessages polled =
                                    client.messages()
                                            .pollMessages(
                                                    stream,
                                                    topic,
                                                    Optional.empty(),
                                                    consumer,
                                                    PollingStrategy.next(),
                                                    config.pollBatchSize,
                                                    true)
                                            .get();
                            if (polled.messages().isEmpty()) {
                                LockSupport.parkNanos(config.emptyPollBackoffMicros * 1000L);
                                continue;
                            }
                            for (Message m : polled.messages()) {
                                callback.messageReceived(m.payload(), m.header().originTimestamp().longValue() / 1000L);
                            }
                        } catch (Exception e) {
                            if (running) {
                                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
                            }
                        }
                    }
                });
    }

    @Override
    public void close() throws Exception {
        running = false;
        loop.shutdown();
        loop.awaitTermination(5, TimeUnit.SECONDS);
        client.close().join();
    }
}
