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
package com.flipkart.ranger.server.bundle.resources;

import com.codahale.metrics.annotation.Metered;
import com.flipkart.ranger.client.RangerHubClient;
import com.flipkart.ranger.core.model.Criteria;
import com.flipkart.ranger.core.model.Service;
import com.flipkart.ranger.core.model.ServiceNode;
import com.flipkart.ranger.server.bundle.model.GenericResponse;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import javax.inject.Inject;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.*;

@Slf4j
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Path("/ranger")
public class RangerResource<T, C extends Criteria<T>> {

    private final List<RangerHubClient<T, C>> rangerHubs;

    @Inject
    public RangerResource(List<RangerHubClient<T, C>> rangerHubs){
        this.rangerHubs = rangerHubs;
    }

    @GET
    @Path("/services/v1")
    @Metered
    public GenericResponse<Collection<Service>> getServices() {
        Collection<Service> services = new ArrayList<>();
        rangerHubs.forEach(hub -> {
            try {
                services.addAll(hub.getServices());
            } catch (Exception e) {
                log.warn("Call to a hub failed with exception, {}", e.getMessage());
            }
        });
        return GenericResponse.<Collection<Service>>builder()
                .success(true)
                .data(services)
                .build();
    }

    @GET
    @Path("/nodes/v1/{namespace}/{serviceName}")
    @Metered
    public GenericResponse<List<ServiceNode<T>>> getNodes(
            @NotNull @NotEmpty @PathParam("namespace") final String namespace,
            @NotNull @NotEmpty @PathParam("serviceName") final String serviceName
    ){
        val service = new Service(namespace, serviceName);
        List<ServiceNode<T>> serviceNodes = new ArrayList<>();
        rangerHubs.forEach(hub -> {
            try {
                serviceNodes.addAll(hub.getAllNodes(service).orElse(Collections.emptyList()));
            } catch (Exception e) {
                log.warn("Call to a hub failed with exception, {}", e.getMessage());
            }
        });
        return GenericResponse.<List<ServiceNode<T>>>builder()
                .success(true)
                .data(serviceNodes)
                .build();
    }
}
