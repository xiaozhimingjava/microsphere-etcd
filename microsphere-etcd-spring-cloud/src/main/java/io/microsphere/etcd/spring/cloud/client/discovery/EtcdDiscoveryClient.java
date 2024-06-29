/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.microsphere.etcd.spring.cloud.client.discovery;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.watch.WatchEvent;
import io.etcd.jetcd.watch.WatchResponse;
import io.microsphere.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.cloud.client.DefaultServiceInstance;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.microsphere.etcd.spring.cloud.client.util.KVClientUtils.buildServicePath;
import static io.microsphere.etcd.spring.cloud.client.util.KVClientUtils.resolveServiceId;
import static io.microsphere.etcd.spring.cloud.client.util.KVClientUtils.toByteSequence;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;

/**
 * Spring Cloud {@link DiscoveryClient} for etcd
 *
 * @author <a href="mailto:mercyblitz@gmail.com">Mercy</a>
 * @since DiscoveryClient
 */
public class EtcdDiscoveryClient implements DiscoveryClient, DisposableBean {

    private static final Logger logger = LoggerFactory.getLogger(EtcdDiscoveryClient.class);

    private static final String rootPath = "/services";

    private final KV kv;

    private final Watch watch;

    private final ObjectMapper objectMapper;

    private final ConcurrentMap<String, List<ServiceInstance>> serviceInstancesCache;

    public EtcdDiscoveryClient(KV kv, Watch watch, ObjectMapper objectMapper) {
        this.kv = kv;
        this.watch = watch;
        this.objectMapper = objectMapper;
        this.serviceInstancesCache = new ConcurrentHashMap<>(16);
    }

    @Override
    public String description() {
        return "Spring Cloud DiscoveryClient for etcd";
    }

    @Override
    public List<ServiceInstance> getInstances(String serviceId) {
        // Sync Load in the first time
        // Cache if hit
        // Async update cache based on Watch Events
        return unmodifiableList(serviceInstancesCache.computeIfAbsent(serviceId, this::doGetInstances));
    }

    protected List<ServiceInstance> doGetInstances(String serviceId) {
        String servicePath = buildServicePath(rootPath, serviceId);
        List<KeyValue> keyValues = getKeyValues(servicePath, false);
        List<ServiceInstance> serviceInstances = keyValues.stream()
                .map(KeyValue::getValue)
                .map(this::deserialize)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        watchService(servicePath, serviceId);
        return serviceInstances;
    }

    private void watchService(String servicePath, String serviceId) {
        ByteSequence key = toByteSequence(servicePath);
        WatchOption.Builder builder = WatchOption.newBuilder().isPrefix(true);
        watch.watch(key, builder.build(), new Watch.Listener() {
            @Override
            public void onNext(WatchResponse response) {
                response.getEvents().forEach(event -> {
                    logger.info("WatchEvent : " + event);
                    WatchEvent.EventType eventType = event.getEventType();
                    switch (eventType) {
                        case PUT:
                            addOrUpdateServiceInstance(event, serviceId);
                            break;
                        case DELETE:
                            deleteServiceInstance(event, serviceId);
                            break;
                        default:
                            logger.warn("Unknown Event Type : " + eventType);
                            break;
                    }
                });
            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onCompleted() {
                logger.info("onCompleted()");
            }
        });
    }

    private void addOrUpdateServiceInstance(WatchEvent event, String serviceId) {
        KeyValue currentKeyValue = event.getKeyValue();
        String instanceId = getInstanceId(currentKeyValue, serviceId);
        ServiceInstance serviceInstance = getServiceInstance(currentKeyValue);
        synchronized (this) { // TODO: Optimization Lock
            List<ServiceInstance> serviceInstances = serviceInstancesCache.computeIfAbsent(serviceId, i -> new LinkedList<>());

            if (isAddServiceInstance(event)) { // Add
                serviceInstances.add(serviceInstance);
            } else { // Update
                int index = -1;
                int size = serviceInstances.size();
                if (size > 0) {
                    serviceInstances.add(serviceInstance);
                    for (int i = 0; i < size; i++) {
                        ServiceInstance previousServiceInstance = serviceInstances.get(i);
                        if (instanceId.equals(previousServiceInstance.getInstanceId())) {
                            index = i;
                            break;
                        }
                    }
                    if (index > -1) {
                        serviceInstances.set(index, serviceInstance);
                        return;
                    }
                }
                serviceInstances.add(serviceInstance);
            }
        }

    }

    private String getInstanceId(KeyValue keyValue, String serviceId) {
        ByteSequence key = keyValue.getKey();
        String serviceInstancePath = key.toString();
        String servicePath = buildServicePath(rootPath, serviceId);
        return StringUtils.substringAfter(serviceInstancePath, servicePath);
    }

    private ServiceInstance getServiceInstance(KeyValue currentKeyValue) {
        ByteSequence value = currentKeyValue.getValue();
        return deserialize(value);
    }

    private boolean isAddServiceInstance(WatchEvent event) {
        KeyValue previousKeyValue = event.getPrevKV();
        return previousKeyValue == null || previousKeyValue.getKey().isEmpty();
    }

    private void deleteServiceInstance(WatchEvent event, String serviceId) {
        KeyValue currentKeyValue = event.getKeyValue();
        String instanceId = getInstanceId(currentKeyValue, serviceId);
        synchronized (this) { // TODO: Optimization Lock
            List<ServiceInstance> serviceInstances = serviceInstancesCache.get(serviceId);
            if (!CollectionUtils.isEmpty(serviceInstances)) {
                Iterator<ServiceInstance> iterator = serviceInstances.iterator();
                while (iterator.hasNext()) {
                    ServiceInstance serviceInstance = iterator.next();
                    if (instanceId.equals(serviceInstance.getInstanceId())) {
                        iterator.remove();
                    }
                }
            }
        }
    }

    private DefaultServiceInstance deserialize(ByteSequence value) {
        byte[] content = value.getBytes();
        DefaultServiceInstance serviceInstance = null;
        try {
            // FIXME Bug on Jackson
            serviceInstance = objectMapper.readValue(content, DefaultServiceInstance.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return serviceInstance;
    }

    @Override
    public List<String> getServices() {
        String rootPath = "/services";

        List<KeyValue> keyValues = getKeyValues(rootPath, true);

        List<String> services = keyValues.stream().map(KeyValue::getKey)
                .map(ByteSequence::toString)
                .map(path -> resolveServiceId(path, rootPath))
                .distinct()
                .collect(Collectors.toList());
        return services;
    }

    private List<KeyValue> getKeyValues(String path, boolean isKeysOnly) {
        ByteSequence key = toByteSequence(path);
        GetOption.Builder builder = GetOption.newBuilder()
                .withKeysOnly(isKeysOnly)
                .isPrefix(true);
        CompletableFuture<GetResponse> getResponseFuture = kv.get(key, builder.build());
        List<KeyValue> keyValues = emptyList();
        try {
            GetResponse response = getResponseFuture.get(1, TimeUnit.SECONDS);
            keyValues = response.getKvs();
        } catch (Throwable e) {
        }
        return keyValues;
    }

    @Override
    public void probe() {
        // TODO

    }

    @Override
    public int getOrder() {
        return 1;
    }

    @Override
    public void destroy() throws Exception {
        this.kv.close();
    }
}
