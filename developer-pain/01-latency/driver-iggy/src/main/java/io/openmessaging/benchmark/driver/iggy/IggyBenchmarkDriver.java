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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkDriver;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.iggy.Iggy;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClient;
import org.apache.iggy.identifier.ConsumerId;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.topic.CompressionAlgorithm;

/**
 * OpenMessaging Benchmark driver for Apache Iggy.
 *
 * <p>Each benchmark topic becomes an Iggy topic inside one stream. Each producer and each consumer
 * gets its own TCP connection, as a separate client process would in production. Subscriptions map
 * to Iggy consumer groups, so consumers that share a subscription split the partitions between them.
 */
public class IggyBenchmarkDriver implements BenchmarkDriver {

    private static final ObjectMapper MAPPER =
            new ObjectMapper(new YAMLFactory())
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private IggyConfig config;
    private AsyncIggyTcpClient admin;
    private final List<AutoCloseable> resources = new CopyOnWriteArrayList<>();

    @Override
    public void initialize(File configurationFile, StatsLogger statsLogger) throws IOException {
        config = MAPPER.readValue(configurationFile, IggyConfig.class);
        admin = connect().join();
        admin.streams()
                .getStream(StreamId.of(config.streamName))
                .thenCompose(
                        existing ->
                                existing.isPresent()
                                        ? CompletableFuture.completedFuture(null)
                                        : admin.streams().createStream(config.streamName).thenApply(s -> null))
                .join();
    }

    CompletableFuture<AsyncIggyTcpClient> connect() {
        return Iggy.tcpClientBuilder()
                .async()
                .host(config.host)
                .port(config.port)
                .credentials(config.username, config.password)
                .buildAndLogin();
    }

    @Override
    public String getTopicNamePrefix() {
        return "omb-iggy";
    }

    @Override
    public CompletableFuture<Void> createTopic(String topic, int partitions) {
        // Expiry and max size of zero mean the server defaults; retention is not part of the benchmark.
        return admin.topics()
                .createTopic(
                        StreamId.of(config.streamName),
                        (long) partitions,
                        CompressionAlgorithm.None,
                        BigInteger.ZERO,
                        BigInteger.ZERO,
                        topic)
                .thenApply(t -> null);
    }

    @Override
    public CompletableFuture<BenchmarkProducer> createProducer(String topic) {
        return connect()
                .thenApply(
                        client -> {
                            IggyBenchmarkProducer p =
                                    new IggyBenchmarkProducer(client, StreamId.of(config.streamName), TopicId.of(topic));
                            resources.add(p);
                            return p;
                        });
    }

    @Override
    public CompletableFuture<BenchmarkConsumer> createConsumer(
            String topic, String subscriptionName, ConsumerCallback consumerCallback) {
        StreamId stream = StreamId.of(config.streamName);
        TopicId topicId = TopicId.of(topic);
        return connect()
                .thenCompose(
                        client ->
                                client.consumerGroups()
                                        .createConsumerGroup(stream, topicId, subscriptionName)
                                        // the group may already exist when several consumers share a subscription
                                        .handle((created, error) -> null)
                                        .thenCompose(
                                                v ->
                                                        client.consumerGroups()
                                                                .joinConsumerGroup(stream, topicId, ConsumerId.of(subscriptionName)))
                                        .thenApply(
                                                v -> {
                                                    IggyBenchmarkConsumer c =
                                                            new IggyBenchmarkConsumer(
                                                                    client, stream, topicId, subscriptionName, config, consumerCallback);
                                                    resources.add(c);
                                                    return c;
                                                }));
    }

    @Override
    public void close() throws Exception {
        for (AutoCloseable r : resources) {
            r.close();
        }
        if (admin != null) {
            admin.close().join();
        }
    }
}
