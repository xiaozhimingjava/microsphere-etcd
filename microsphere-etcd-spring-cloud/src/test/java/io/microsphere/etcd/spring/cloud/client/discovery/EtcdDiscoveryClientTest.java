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
import io.etcd.jetcd.Client;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

/**
 * EtcdDiscoveryClient
 *
 * @author <a href="mailto:mercyblitz@gmail.com">Mercy</a>
 * @since 1.0.0
 */
public class EtcdDiscoveryClientTest {

    private static Client client;

    private static EtcdDiscoveryClient discoveryClient;

    @BeforeAll
    public static void prepare() {
        client = Client.builder().endpoints("http://127.0.0.1:2379").build();
        discoveryClient = new EtcdDiscoveryClient(client.getKVClient(), new ObjectMapper());
    }

    @AfterAll
    public static void clear() throws Exception {
        client.close();
        discoveryClient.destroy();
    }

    @Test
    public void testGetOrder() {
        assertEquals(1, discoveryClient.getOrder());
    }

    @Test
    public void testGetServices() {
        List<String> services = discoveryClient.getServices();
        assertFalse(services.isEmpty());
    }

    @Test
    public void testGetInstances() {
        List<String> services = discoveryClient.getServices();
        services.stream().map(discoveryClient::getInstances)
                .forEach(instances -> {
                    assertNotNull(instances);
                });
    }

}
