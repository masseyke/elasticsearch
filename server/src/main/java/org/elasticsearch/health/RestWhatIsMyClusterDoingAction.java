/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

@ServerlessScope(Scope.INTERNAL)
public class RestWhatIsMyClusterDoingAction extends BaseRestHandler {

    private static final String VERBOSE_PARAM = "verbose";

    private static final String SIZE_PARAM = "size";

    @Override
    public String getName() {
        return "what_is_my_cluster_doing";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_whats_going_on"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        boolean demoMode = request.paramAsBoolean("demo", false);
        int demoOffset = request.paramAsInt("demo_number", 0);
        String mode = request.param("mode", "standard");
        String node = request.param("node");
        WhatIsMyClusterDoingAction.Request getHealthRequest = new WhatIsMyClusterDoingAction.Request(
            WhatIsMyClusterDoingAction.Request.Mode.valueOf(mode.toUpperCase()),
            node,
            demoMode,
            demoOffset
        );
        return channel -> client.execute(WhatIsMyClusterDoingAction.INSTANCE, getHealthRequest, new RestToXContentListener<>(channel));
    }

}
