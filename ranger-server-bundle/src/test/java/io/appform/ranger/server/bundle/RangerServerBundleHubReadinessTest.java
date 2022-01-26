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
package io.appform.ranger.server.bundle;

import static io.appform.ranger.client.utils.RangerHubTestUtils.service;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheck;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.appform.ranger.client.RangerHubClient;
import io.appform.ranger.client.stubs.RangerTestHub;
import io.appform.ranger.client.stubs.TestCriteria;
import io.appform.ranger.client.stubs.TestDeserializer;
import io.appform.ranger.client.stubs.TestServiceFinderFactory;
import io.appform.ranger.client.stubs.TestSimpleUnshardedServiceFinder;
import io.appform.ranger.core.finder.ServiceFinder;
import io.appform.ranger.core.finder.SimpleUnshardedServiceFinder;
import io.appform.ranger.core.finder.serviceregistry.ListBasedServiceRegistry;
import io.appform.ranger.core.finder.serviceregistry.signal.ScheduledRegistryUpdateSignal;
import io.appform.ranger.core.finderhub.ServiceFinderFactory;
import io.appform.ranger.core.model.Deserializer;
import io.appform.ranger.core.model.Service;
import io.appform.ranger.core.signals.Signal;
import io.appform.ranger.core.units.TestNodeData;
import io.appform.ranger.core.utils.RangerTestUtils;
import io.dropwizard.Configuration;
import io.dropwizard.jersey.DropwizardResourceConfig;
import io.dropwizard.jersey.setup.JerseyEnvironment;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import io.dropwizard.setup.AdminEnvironment;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.experimental.SuperBuilder;
import lombok.val;
import lombok.var;
import org.eclipse.jetty.util.component.LifeCycle;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RangerServerBundleHubReadinessTest {

    private final JerseyEnvironment jerseyEnvironment = mock(JerseyEnvironment.class);
    private final MetricRegistry metricRegistry = mock(MetricRegistry.class);
    private final LifecycleEnvironment lifecycleEnvironment = new LifecycleEnvironment(metricRegistry);
    private final Environment environment = mock(Environment.class);
    private final Bootstrap<?> bootstrap = mock(Bootstrap.class);
    private final Configuration configuration = mock(Configuration.class);

    private final RangerServerBundle<TestNodeData, ListBasedServiceRegistry<TestNodeData>, Configuration>
        rangerServerBundle = new RangerServerBundle<TestNodeData, ListBasedServiceRegistry<TestNodeData>, Configuration>() {

        @Override
        protected List<RangerHubClient<TestNodeData, ListBasedServiceRegistry<TestNodeData>>> withHubs(Configuration configuration) {
            return Collections.singletonList(getTestHub());
        }

        @Override
        protected List<HealthCheck> withHealthChecks(Configuration configuration) {
            return Collections.emptyList();
        }

        @Override
        protected boolean waitTillAllHubsAreReadyToUse(Configuration configuration) {
            return false;
        }
    };

    @Before
    public void setup() throws Exception {
        when(jerseyEnvironment.getResourceConfig()).thenReturn(new DropwizardResourceConfig());
        when(environment.jersey()).thenReturn(jerseyEnvironment);
        when(environment.lifecycle()).thenReturn(lifecycleEnvironment);
        when(environment.getObjectMapper()).thenReturn(new ObjectMapper());
        AdminEnvironment adminEnvironment = mock(AdminEnvironment.class);
        doNothing().when(adminEnvironment).addTask(any());
        when(environment.admin()).thenReturn(adminEnvironment);

        rangerServerBundle.initialize(bootstrap);
        rangerServerBundle.run(configuration, environment);
        for (val lifeCycle : lifecycleEnvironment.getManagedObjects()) {
            lifeCycle.start();
        }
    }

    @Test
    public void testRangerServerBundleStartupWhenHubIsNotReady() {
        var hub = rangerServerBundle.getHubs().get(0);
        Assert.assertTrue(hub instanceof RangerTestHubWithCustomFinderFactory);
        RangerTestUtils.sleepUntilFindersAreCreated(hub.getHub());

        // Though initialRefreshCompleted is false since the check is disabled in rangerServerBundle, bundle has run without checking
        Assert.assertFalse(hub.getHub().getFinders().get().get(service).getServiceRegistry().getInitialRefreshCompleted().get());
    }

    @After
    public void tearDown() throws Exception {
        for (LifeCycle lifeCycle : lifecycleEnvironment.getManagedObjects()) {
            lifeCycle.stop();
        }
    }

    private RangerTestHubWithCustomFinderFactory getTestHub() {
        return RangerTestHubWithCustomFinderFactory.builder()
            .namespace(service.getNamespace())
            .mapper(new ObjectMapper())
            .nodeRefreshTimeMs(1000)
            .initialCriteria(new TestCriteria())
            .deserializer(new TestDeserializer<>())
            .build();
    }

    @SuperBuilder
    static class RangerTestHubWithCustomFinderFactory extends RangerTestHub {
        @Override
        protected ServiceFinderFactory<TestNodeData, ListBasedServiceRegistry<TestNodeData>> buildFinderFactory() {
            return new TestServiceFinderFactoryWithCustomFinder();
        }
    }

    static class TestServiceFinderFactoryWithCustomFinder extends TestServiceFinderFactory {
        @Override
        public ServiceFinder<TestNodeData, ListBasedServiceRegistry<TestNodeData>> buildFinder(
            Service service) {
            val finder = new TestSimpleUnshardedServiceFinderWithCustomBuilder<TestNodeData>()
                .withNamespace(service.getNamespace())
                .withServiceName(service.getServiceName())
                .withDeserializer(new Deserializer<TestNodeData>() {})
                .build();
            finder.start();
            return finder;
        }
    }

    static class TestSimpleUnshardedServiceFinderWithCustomBuilder<T>
        extends TestSimpleUnshardedServiceFinder<T> {

        @Override
        public SimpleUnshardedServiceFinder<TestNodeData> build() {

            val service = Service.builder().namespace(namespace).serviceName(serviceName).build();
            val finder = buildFinder(service, shardSelector, nodeSelector);
            val signalGenerators = new ArrayList<Signal<T>>();
            val nodeDataSource = dataSource(service);

            signalGenerators.add(new ScheduledRegistryUpdateSignal<>(service, nodeRefreshIntervalMs));

            finder.getStartSignal()
                .registerConsumers(startSignalHandlers)
                .registerConsumer(x -> nodeDataSource.start());

            finder.getStopSignal()
                .registerConsumer(x -> signalGenerators.forEach(Signal::stop))
                .registerConsumer(x -> nodeDataSource.stop());
            return finder;
        }
    }

}
