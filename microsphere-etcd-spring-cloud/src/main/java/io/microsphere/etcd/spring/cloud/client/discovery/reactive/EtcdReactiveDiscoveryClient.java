package io.microsphere.etcd.spring.cloud.client.discovery.reactive;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.KeyValue;
import io.microsphere.etcd.spring.cloud.client.discovery.EtcdClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.ReactiveDiscoveryClient;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.util.List;
import java.util.stream.Collectors;

import static io.microsphere.etcd.spring.cloud.client.util.KVClientUtils.resolveServiceId;

/**
 * A {@link ReactiveDiscoveryClient} implementation for Etcd.
 *
 * @author xzm
 */
public class EtcdReactiveDiscoveryClient implements ReactiveDiscoveryClient {


    private static final Logger logger = LoggerFactory.getLogger(EtcdReactiveDiscoveryClient.class);

    private final EtcdClient etcdClient;


    public EtcdReactiveDiscoveryClient(EtcdClient etcdClient) {
        this.etcdClient = etcdClient;
    }

    @Override
    public String description() {
        return "Spring Cloud Etcd Reactive Discovery Client";
    }

    @Override
    public Flux<ServiceInstance> getInstances(String serviceId) {
        return Flux.defer(() -> Flux.fromIterable(etcdClient.doGetInstances(serviceId)))
                .subscribeOn(Schedulers.boundedElastic());
    }


    @Override
    public Flux<String> getServices() {
        return Flux.defer(() -> {
            String rootPath = "/services";

            List<KeyValue> keyValues = etcdClient.getKeyValues(rootPath, true);

            List<String> services = keyValues.stream().map(KeyValue::getKey)
                    .map(ByteSequence::toString)
                    .map(path -> resolveServiceId(path, rootPath))
                    .distinct()
                    .collect(Collectors.toList());
            return services == null ? Flux.empty() : Flux.fromIterable(services);
        }).onErrorResume(exception -> {
            logger.error("Error getting services from Etcd.", exception);
            return Flux.empty();
        }).subscribeOn(Schedulers.boundedElastic());
    }
}