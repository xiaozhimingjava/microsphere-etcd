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
package io.microsphere.etcd.spring.cloud.client.discovery.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.ClientBuilder;
import io.microsphere.etcd.spring.cloud.client.EtcdClientProperties;
import io.microsphere.etcd.spring.cloud.client.discovery.EtcdDiscoveryClient;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.util.Set;

/**
 * The Auto-Configuration class for Etcd Service Discovery
 *
 * @author <a href="mailto:mercyblitz@gmail.com">Mercy</a>
 * @see EtcdClientProperties
 * @see EtcdDiscoveryClient
 * @since 1.0.0
 */
@EnableConfigurationProperties(EtcdClientProperties.class)
public class EtcdDiscoveryAutoConfiguration {

    @Bean(destroyMethod = "close")
    public Client etcdClient(EtcdClientProperties etcdClientProperties) {
        Set<String> endpoints = etcdClientProperties.getEndpoints();
        ClientBuilder clientBuilder = Client.builder().endpoints(endpoints.toArray(new String[0]));
        // TODO More Configurations
        return clientBuilder.build();
    }

    @Bean
    public EtcdDiscoveryClient etcdDiscoveryClient(Client client, EtcdClientProperties etcdClientProperties
            , ObjectMapper objectMapper) {
        return new EtcdDiscoveryClient(client, etcdClientProperties, objectMapper);
    }
}
