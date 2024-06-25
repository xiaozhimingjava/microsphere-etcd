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
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.options.GetOption;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.cloud.client.DefaultServiceInstance;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.microsphere.etcd.spring.cloud.client.util.KVClientUtils.buildServicePath;
import static io.microsphere.etcd.spring.cloud.client.util.KVClientUtils.resolveServiceId;
import static io.microsphere.etcd.spring.cloud.client.util.KVClientUtils.toByteSequence;
import static java.util.Collections.emptyList;

/**
 * Spring Cloud {@link DiscoveryClient} for etcd
 *
 * @author <a href="mailto:mercyblitz@gmail.com">Mercy</a>
 * @since DiscoveryClient
 */
public class EtcdDiscoveryClient implements DiscoveryClient, DisposableBean {

    private final KV kv;

    private final ObjectMapper objectMapper;

    public EtcdDiscoveryClient(KV kv, ObjectMapper objectMapper) {
        this.kv = kv;
        this.objectMapper = objectMapper;
    }

    @Override
    public String description() {
        return "Spring Cloud DiscoveryClient for etcd";
    }

    @Override
    public List<ServiceInstance> getInstances(String serviceId) {
        String rootPath = "/services";
        String servicePath = buildServicePath(rootPath, serviceId);
        List<KeyValue> keyValues = getKeyValues(servicePath, false);
        List<ServiceInstance> serviceInstances = keyValues.stream()
                .map(KeyValue::getValue)
                .map(this::deserialize)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        return serviceInstances;
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
