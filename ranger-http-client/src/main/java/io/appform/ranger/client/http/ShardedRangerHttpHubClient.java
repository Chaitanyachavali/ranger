/*
 * Copyright 2015 Flipkart Internet Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.appform.ranger.client.http;

import io.appform.ranger.client.AbstractRangerHubClient;
import io.appform.ranger.core.finder.nodeselector.RoundRobinServiceNodeSelector;
import io.appform.ranger.core.finder.serviceregistry.MapBasedServiceRegistry;
import io.appform.ranger.core.finder.shardselector.MatchingShardSelector;
import io.appform.ranger.core.finderhub.ServiceDataSource;
import io.appform.ranger.core.finderhub.ServiceFinderFactory;
import io.appform.ranger.core.finderhub.ServiceFinderHub;
import io.appform.ranger.http.config.HttpClientConfig;
import io.appform.ranger.http.serde.HTTPResponseDataDeserializer;
import io.appform.ranger.http.servicefinderhub.HttpServiceDataSource;
import io.appform.ranger.http.servicefinderhub.HttpServiceFinderHubBuilder;
import io.appform.ranger.http.servicefinderhub.HttpShardedServiceFinderFactory;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@SuperBuilder
public class ShardedRangerHttpHubClient<T>
        extends AbstractRangerHubClient<T, MapBasedServiceRegistry<T>, HTTPResponseDataDeserializer<T>> {

    private final HttpClientConfig clientConfig;

    @Override
    protected ServiceDataSource getDataSource() {
        return new HttpServiceDataSource<>(clientConfig, getMapper());
    }

    @Override
    protected ServiceFinderHub<T, MapBasedServiceRegistry<T>> buildHub() {
        return new HttpServiceFinderHubBuilder<T, MapBasedServiceRegistry<T>>()
                .withServiceDataSource(buildServiceDataSource())
                .withServiceFinderFactory(buildFinderFactory())
                .withRefreshFrequencyMs(getNodeRefreshTimeMs())
                .build();
    }

    @Override
    protected ServiceFinderFactory<T, MapBasedServiceRegistry<T>> buildFinderFactory() {
        return HttpShardedServiceFinderFactory.<T>builder()
                .httpClientConfig(clientConfig)
                .nodeRefreshIntervalMs(getNodeRefreshTimeMs())
                .deserializer(getDeserializer())
                .shardSelector(new MatchingShardSelector<>())
                .nodeSelector(new RoundRobinServiceNodeSelector<>())
                .mapper(getMapper())
                .build();
    }

}
