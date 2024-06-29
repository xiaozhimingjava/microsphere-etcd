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
package io.microsphere.etcd.spring.cloud.client;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Collections;
import java.util.Set;

import static java.util.Collections.singleton;

/**
 * Spring Boot {@link ConfigurationProperties @ConfigurationProperties} for etcd Client
 *
 * @author <a href="mailto:mercyblitz@gmail.com">Mercy</a>
 * @see ConfigurationProperties
 * @since 1.0.0
 */
@ConfigurationProperties(prefix = "io.microsphere.etcd.spring.cloud")
public class EtcdClientProperties {

    /**
     * The endpoints for etcd
     */
    private Set<String> endpoints = singleton("http://127.0.0.1:2379");

    /**
     * The prefix of etcd key for Service Discovery and Registration
     */
    private String rootPath = "/services";

    /**
     * The service names(ids) for subscription
     */
    private Set<String> subscribedServices = singleton("*");

    public Set<String> getEndpoints() {
        return endpoints;
    }

    public void setEndpoints(Set<String> endpoints) {
        this.endpoints = endpoints;
    }

    public String getRootPath() {
        return rootPath;
    }

    public void setRootPath(String rootPath) {
        this.rootPath = rootPath;
    }

    public Set<String> getSubscribedServices() {
        return subscribedServices;
    }

    public void setSubscribedServices(Set<String> subscribedServices) {
        this.subscribedServices = subscribedServices;
    }
}
