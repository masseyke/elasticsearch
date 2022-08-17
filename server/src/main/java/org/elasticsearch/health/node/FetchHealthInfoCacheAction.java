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
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

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
        private final Map<
            Class<? extends HealthNodeInfo>,
            Map<String, ? extends HealthNodeInfo>> healthNodeInfoClassToNodeToHealthNodeInfoMap;

        public Response(
            final Map<Class<? extends HealthNodeInfo>, Map<String, ? extends HealthNodeInfo>> healthNodeInfoClassToNodeToHealthNodeInfoMap
        ) {
            this.healthNodeInfoClassToNodeToHealthNodeInfoMap = healthNodeInfoClassToNodeToHealthNodeInfoMap;
        }

        public Response(StreamInput input) throws IOException {
            try {
                this.healthNodeInfoClassToNodeToHealthNodeInfoMap = readMap(input);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }

        @SuppressWarnings({ "unchecked" })
        public Map<Class<? extends HealthNodeInfo>, Map<String, ? extends HealthNodeInfo>> readMap(StreamInput input) throws IOException,
            ClassNotFoundException {
            int size = input.readInt();
            if (size == 0) {
                return Collections.emptyMap();
            }
            Map<Class<? extends HealthNodeInfo>, Map<String, ? extends HealthNodeInfo>> map = new HashMap<>(size);
            for (int i = 0; i < size; i++) {
                final Class<HealthNodeInfo> key = (Class<HealthNodeInfo>) Class.forName(input.readString());
                Map<String, HealthNodeInfo> value = input.readMap(StreamInput::readString, in -> {
                    try {
                        return key.getDeclaredConstructor(StreamInput.class).newInstance(in);
                    } catch (InstantiationException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                        throw new RuntimeException(e);
                    }
                });
                map.put(key, value);
            }
            return map;
        }

        public void writeMap(StreamOutput output, Map<Class<? extends HealthNodeInfo>, Map<String, ? extends HealthNodeInfo>> map)
            throws IOException, ClassNotFoundException {
            int size = map.size();
            output.writeInt(size);
            for (Map.Entry<Class<? extends HealthNodeInfo>, Map<String, ? extends HealthNodeInfo>> entry : map.entrySet()) {
                output.writeString(entry.getKey().getName());
                output.writeMap(entry.getValue(), StreamOutput::writeString, (out, value) -> value.writeTo(out));
            }
            ;
        }

        @Override
        public void writeTo(StreamOutput output) throws IOException {
            try {
                writeMap(output, healthNodeInfoClassToNodeToHealthNodeInfoMap);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }

        @SuppressWarnings({ "unchecked" })
        public <T extends HealthNodeInfo> Map<String, T> getHealthNodeInfo(Class<T> infoClass) {
            if (infoClass == null) {
                return null;
            }
            return (Map<String, T>) healthNodeInfoClassToNodeToHealthNodeInfoMap.get(infoClass);
        }

        @SuppressWarnings({ "unchecked" })
        public Map<String, DiskHealthInfo> getDiskHealthInfoMap() {
            return (Map<String, DiskHealthInfo>) healthNodeInfoClassToNodeToHealthNodeInfoMap.get(DiskHealthInfo.class);
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
            Map<String, DiskHealthInfo> diskHealthInfoMap = nodeHealthOverview.getDiskHealthInfo();
            Map<Class<? extends HealthNodeInfo>, Map<String, ? extends HealthNodeInfo>> map = new HashMap<>();
            map.put(DiskHealthInfo.class, diskHealthInfoMap);
            listener.onResponse(new Response(map));
        }
    }
}
