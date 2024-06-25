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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.etcd.jetcd.Client;
import io.microsphere.spring.cloud.client.service.registry.DefaultRegistration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.UUID;

/**
 * {@link EtcdServiceRegistry} Test
 *
 * @author <a href="mailto:mercyblitz@gmail.com">Mercy</a>
 * @since EtcdServiceRegistry
 */
public class EtcdServiceRegistryTest {

    private static Client client;

    private static EtcdServiceRegistry registry;

    private static DefaultRegistration registration = createRegistration();

    @BeforeAll
    public static void prepare() {
        client = Client.builder().endpoints("http://127.0.0.1:2379").build();
        registry = new EtcdServiceRegistry(client.getKVClient(), new ObjectMapper());
    }

    @AfterAll
    public static void clear() {
        registry.close();
        client.close();
    }

    private static DefaultRegistration createRegistration() {
        DefaultRegistration registration = new DefaultRegistration();
        registration.setInstanceId(UUID.randomUUID().toString());
        registration.setServiceId("test-service");
        registration.setHost("127.0.0.1");
        registration.setPort(8080);
        registration.setUri(URI.create("http://127.0.0.1:8080"));
        return registration;
    }

    @Test
    public void testRegisterAndDeregister() throws InterruptedException {
        registry.register(registration);
        Thread.sleep(3000L);
        registry.deregister(registration);
        Thread.sleep(3000L);
    }




}
