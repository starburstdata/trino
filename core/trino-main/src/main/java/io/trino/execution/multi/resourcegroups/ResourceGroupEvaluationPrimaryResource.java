/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.execution.multi.resourcegroups;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.SessionRepresentation;
import io.trino.dispatcher.DispatchExecutor;
import io.trino.execution.ManagedQueryExecution;
import io.trino.execution.QueryState;
import io.trino.execution.TaskId;
import io.trino.execution.multi.resourcegroups.RemoteManagedQueryExecution.RemoteQueryState;
import io.trino.execution.resourcegroups.ResourceGroupManager;
import io.trino.memory.LowMemoryKiller.RunningTaskInfo;
import io.trino.metadata.InternalNodeManager;
import io.trino.metadata.SessionPropertyManager;
import io.trino.operator.RetryPolicy;
import io.trino.server.security.ResourceSecurity;
import io.trino.spi.QueryId;
import io.trino.spi.resourcegroups.SelectionContext;
import io.trino.spi.resourcegroups.SelectionCriteria;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import org.joda.time.DateTime;

import java.util.Map;

import static io.trino.SystemSessionProperties.getRetryPolicy;
import static io.trino.memory.LowMemoryKiller.RunningTaskInfo.getTaskInfo;
import static io.trino.server.security.ResourceSecurity.AccessType.INTERNAL_ONLY;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@Path("/v1/resourceGroups")
public class ResourceGroupEvaluationPrimaryResource
{
    private final ResourceGroupManager<?> resourceGroupManager;
    private final DispatchExecutor dispatchExecutor;

    private final ResourceGroupEvaluationSecondaryClient resourceGroupEvaluationSecondaryClient;

    private final InternalNodeManager nodeManager;
    private final RemoteQueryTracker remoteQueryTracker;
    private final SessionPropertyManager sessionPropertyManager;

    @Inject
    public ResourceGroupEvaluationPrimaryResource(
            ResourceGroupManager<?> resourceGroupManager,
            DispatchExecutor dispatchExecutor,
            ResourceGroupEvaluationSecondaryClient resourceGroupEvaluationSecondaryClient,
            InternalNodeManager nodeManager,
            RemoteQueryTracker remoteQueryTracker,
            SessionPropertyManager sessionPropertyManager)
    {
        this.resourceGroupManager = requireNonNull(resourceGroupManager, "resourceGroupManager is null");
        this.dispatchExecutor = requireNonNull(dispatchExecutor, "dispatchExecutor is null");
        this.resourceGroupEvaluationSecondaryClient = requireNonNull(resourceGroupEvaluationSecondaryClient, "resourceGroupEvaluationSecondaryClient is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.remoteQueryTracker = requireNonNull(remoteQueryTracker, "remoteQueryTracker is null");
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
    }

    @ResourceSecurity(INTERNAL_ONLY)
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("submit")
    public void submit(SubmitQueryRequest request)
    {
        RemoteManagedQueryExecution remoteManagedQueryExecution = new RemoteManagedQueryExecution(
                resourceGroupEvaluationSecondaryClient,
                nodeManager.getCoordinators()
                        .stream()
                        .filter(coordinator -> coordinator.getNodeIdentifier().equals(request.coordinatorId()))
                        .findAny()
                        .orElseThrow(),
                request.queryId(),
                request.sessionRepresentation().toSession(sessionPropertyManager));
        remoteQueryTracker.add(remoteManagedQueryExecution);
        SelectionContext selectionContext = resourceGroupManager.selectGroup(request.selectionCriteria());

        resourceGroupManager.submit(
                remoteManagedQueryExecution,
                request.selectionCriteria(),
                selectionContext,
                dispatchExecutor.getExecutor());
    }

    @ResourceSecurity(INTERNAL_ONLY)
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("queryStateChanged")
    public void queryStateChanged(QueryResourceGroupState queryResourceGroupState)
    {
        RemoteManagedQueryExecution queryExecution = remoteQueryTracker.get(queryResourceGroupState.queryId());
        // setRemoteQueryState will run all callbacks
        queryExecution.setRemoteQueryState(queryResourceGroupState.toRemoteQueryState());
    }

    @JsonSerialize
    public record SubmitQueryRequest(String coordinatorId, QueryId queryId, int queryPriority, SelectionCriteria selectionCriteria, SessionRepresentation sessionRepresentation)
    {
        public SubmitQueryRequest
        {
            requireNonNull(coordinatorId, "coordinatorId is null");
            requireNonNull(queryId, "queryId is null");
            requireNonNull(selectionCriteria, "selectionCriteria is null");
            requireNonNull(sessionRepresentation, "sessionRepresentation is null");
        }
    }

    public record QueryResourceGroupState(
            QueryId queryId,
            QueryState state,
            long totalCpuTimeNanos,
            long totalMemoryReservationBytes,
            long userMemoryReservationBytes,
            Map<TaskId, RunningTaskInfo> taskInfo)
    {
        public QueryResourceGroupState
        {
            requireNonNull(queryId, "queryId is null");
            requireNonNull(state, "state is null");
            requireNonNull(taskInfo, "taskInfo is null");
        }

        public static QueryResourceGroupState fromQuery(ManagedQueryExecution query)
        {
            return new QueryResourceGroupState(
                    query.getBasicQueryInfo().getQueryId(),
                    query.getState(),
                    query.getTotalCpuTime().roundTo(NANOSECONDS),
                    query.getTotalMemoryReservation().toBytes(),
                    query.getBasicQueryInfo().getQueryStats().getUserMemoryReservation().toBytes(),
                    // task info is only needed for FTE
                    getRetryPolicy(query.getSession()) == RetryPolicy.TASK ? getTaskInfo(query.getFullQueryInfo()) : ImmutableMap.of());
        }

        public RemoteQueryState toRemoteQueryState()
        {
            return new RemoteQueryState(
                    state,
                    new Duration(totalCpuTimeNanos, NANOSECONDS),
                    DataSize.ofBytes(totalMemoryReservationBytes),
                    DataSize.ofBytes(userMemoryReservationBytes),
                    DateTime.now(),
                    taskInfo);
        }
    }
}
