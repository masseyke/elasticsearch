/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.node;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.health.node.action.TransportHealthNodeAction;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class FetchHealthInfoCacheAction extends ActionType<FetchHealthInfoCacheAction.Response> {

    public static class Request extends ActionRequest {
        public Request() {}

        public Request(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class Response extends ActionResponse {
        /*
         * This maps the HealthNodeInfo class to a Map of node name to the HealthNodeInfo object of that class for that node
         */
        private final Map<String, DiskHealthInfo> diskInfoByNode;

        public Response(final Map<String, DiskHealthInfo> diskInfoByNode) {
            this.diskInfoByNode = diskInfoByNode;
        }

        public Response(StreamInput input) throws IOException {
            this.diskInfoByNode = input.readMap(StreamInput::readString, DiskHealthInfo::new);
        }

        @Override
        public void writeTo(StreamOutput output) throws IOException {
            output.writeMap(diskInfoByNode, StreamOutput::writeString, (out, diskHealthInfo) -> diskHealthInfo.writeTo(out));
        }

        public Map<String, DiskHealthInfo> getDiskHealthInfo() {
            return Map.copyOf(diskInfoByNode);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FetchHealthInfoCacheAction.Response response = (FetchHealthInfoCacheAction.Response) o;
            return diskInfoByNode.equals(response.diskInfoByNode);
        }

        @Override
        public int hashCode() {
            return Objects.hash(diskInfoByNode);
        }
    }

    public static final FetchHealthInfoCacheAction INSTANCE = new FetchHealthInfoCacheAction();
    public static final String NAME = "cluster:monitor/fetch/health/info";

    private FetchHealthInfoCacheAction() {
        super(NAME, FetchHealthInfoCacheAction.Response::new);
    }

    public static class TransportAction extends TransportHealthNodeAction<
        FetchHealthInfoCacheAction.Request,
        FetchHealthInfoCacheAction.Response> {
        private final HealthInfoCache nodeHealthOverview;

        @Inject
        public TransportAction(
            TransportService transportService,
            ClusterService clusterService,
            ThreadPool threadPool,
            ActionFilters actionFilters,
            HealthInfoCache nodeHealthOverview
        ) {
            super(
                FetchHealthInfoCacheAction.NAME,
                transportService,
                clusterService,
                threadPool,
                actionFilters,
                FetchHealthInfoCacheAction.Request::new,
                FetchHealthInfoCacheAction.Response::new,
                ThreadPool.Names.MANAGEMENT
            );
            this.nodeHealthOverview = nodeHealthOverview;
        }

        @Override
        protected void healthOperation(
            Task task,
            FetchHealthInfoCacheAction.Request request,
            ClusterState clusterState,
            ActionListener<FetchHealthInfoCacheAction.Response> listener
        ) {
            listener.onResponse(new Response(nodeHealthOverview.getDiskHealthInfo()));
        }
    }
}
