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
package io.microsphere.etcd.spring.cloud.client.service.registry;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.kv.DeleteResponse;
import io.etcd.jetcd.kv.PutResponse;
import io.microsphere.spring.cloud.client.service.registry.DefaultRegistration;
import org.slf4j.Logger;
import org.springframework.cloud.client.serviceregistry.ServiceRegistry;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static io.microsphere.etcd.spring.cloud.client.util.KVClientUtils.buildServiceInstancePath;
import static io.microsphere.etcd.spring.cloud.client.util.KVClientUtils.toByteSequence;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Spring Cloud {@link ServiceRegistry} for etcd
 *
 * @author <a href="mailto:mercyblitz@gmail.com">Mercy</a>
 * @see DefaultRegistration
 * @see ServiceRegistry
 * @since 1.0.0
 */
public class EtcdServiceRegistry implements ServiceRegistry<DefaultRegistration> {

    private static final Logger logger = getLogger(EtcdServiceRegistry.class);

    private static final String STATUS_METADATA_KEY = "status";

    private final KV kv;

    private final ObjectMapper objectMapper;

    public EtcdServiceRegistry(KV kv, ObjectMapper objectMapper) {
        this.kv = kv;
        this.objectMapper = objectMapper;
    }

    @Override
    public void register(DefaultRegistration registration) {
        String path = resolvePath(registration);
        String json = deserialize(registration);
        put(path, json);
    }

    @Override
    public void deregister(DefaultRegistration registration) {
        String path = resolvePath(registration);
        delete(path);
    }

    @Override
    public void close() {
        // TODO
        kv.close();
    }

    @Override
    public void setStatus(DefaultRegistration registration, String status) {
        Map<String, String> metadata = registration.getMetadata();
        metadata.put(STATUS_METADATA_KEY, status);
        register(registration);
    }

    @Override
    public <T> T getStatus(DefaultRegistration registration) {
        Map<String, String> metadata = registration.getMetadata();
        return (T) metadata.get(STATUS_METADATA_KEY);
    }

    private String resolvePath(DefaultRegistration registration) {
        String rootPath = "/services";
        String serviceId = registration.getServiceId();
        String instanceId = registration.getInstanceId();
        return buildServiceInstancePath(rootPath, serviceId, instanceId);
    }

    private String deserialize(DefaultRegistration registration) {
        String json = null;
        try {
            json = objectMapper.writeValueAsString(registration);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return json;
    }

    private void put(String path, String json) {
        ByteSequence key = toByteSequence(path);
        ByteSequence value = toByteSequence(json);
        CompletableFuture<PutResponse> putResponse = kv.put(key, value);
    }

    private void delete(String path) {
        ByteSequence key = toByteSequence(path);
        CompletableFuture<DeleteResponse> deleteResponse = kv.delete(key);
    }

}
