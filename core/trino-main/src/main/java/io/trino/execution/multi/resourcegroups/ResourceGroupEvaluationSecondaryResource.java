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

import com.google.inject.Inject;
import io.trino.dispatcher.DispatchManager;
import io.trino.execution.QueryManager;
import io.trino.execution.multi.resourcegroups.ResourceGroupEvaluationPrimaryResource.QueryResourceGroupState;
import io.trino.execution.multi.resourcegroups.ResourceGroupEvaluationSecondaryClient.FailQueryRequest;
import io.trino.execution.multi.resourcegroups.ResourceGroupEvaluationSecondaryClient.FailTaskRequest;
import io.trino.execution.multi.resourcegroups.ResourceGroupEvaluationSecondaryClient.QueryStateResponse;
import io.trino.server.security.ResourceSecurity;
import io.trino.spi.QueryId;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.server.security.ResourceSecurity.AccessType.INTERNAL_ONLY;
import static java.util.Objects.requireNonNull;

@Path("/v1/resourceGroups")
public class ResourceGroupEvaluationSecondaryResource
{
    private final DispatchManager dispatchManager;
    private final QueryManager queryManager;

    @Inject
    public ResourceGroupEvaluationSecondaryResource(DispatchManager dispatchManager, QueryManager queryManager)
    {
        this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
    }

    @ResourceSecurity(INTERNAL_ONLY)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("startQuery/{queryId}")
    public void startWaitingForResources(@PathParam("queryId") QueryId queryId)
    {
        dispatchManager.startWaitingForResources(queryId);
    }

    @ResourceSecurity(INTERNAL_ONLY)
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Path("fail")
    public void fail(FailQueryRequest failQueryRequest)
    {
        dispatchManager.failQuery(failQueryRequest.queryId(), failQueryRequest.cause().toException());
    }

    @ResourceSecurity(INTERNAL_ONLY)
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Path("failTask")
    public void fail(FailTaskRequest failTaskRequest)
    {
        queryManager.failTask(failTaskRequest.taskId(), failTaskRequest.cause().toException());
    }

    @ResourceSecurity(INTERNAL_ONLY)
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Path("queryState")
    public QueryStateResponse queryState(ResourceGroupEvaluationSecondaryClient.QueryStateRequest request)
    {
        return new QueryStateResponse(request
                .queries()
                .stream()
                .map(queryId -> QueryResourceGroupState.fromQuery(dispatchManager.getQuery(queryId)))
                .collect(toImmutableList()));
    }
}
