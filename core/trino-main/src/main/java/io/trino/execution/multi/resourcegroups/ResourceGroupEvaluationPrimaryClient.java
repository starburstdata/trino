package io.trino.execution.multi.resourcegroups;

import com.google.inject.Inject;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.trino.Session;
import io.trino.SessionRepresentation;
import io.trino.execution.ManagedQueryExecution;
import io.trino.execution.multi.resourcegroups.ResourceGroupEvaluationPrimaryResource.QueryResourceGroupState;
import io.trino.execution.multi.resourcegroups.ResourceGroupEvaluationPrimaryResource.SubmitQueryRequest;
import io.trino.metadata.InternalNodeManager;
import io.trino.spi.QueryId;
import io.trino.spi.resourcegroups.SelectionCriteria;
import org.jetbrains.annotations.NotNull;

import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.trino.execution.multi.resourcegroups.StatusCodeCheckResponseHandler.checkResponseStatusCode;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class ResourceGroupEvaluationPrimaryClient
{
    private final HttpClient httpClient;
    private final String currentNodeId;
    private final InternalNodeManager nodeManager;
    private final JsonCodec<SubmitQueryRequest> submitQueryRequestCodec;
    private final JsonCodec<QueryResourceGroupState> queryResourceGroupStateCodec;

    @Inject
    public ResourceGroupEvaluationPrimaryClient(
            @ForRemoteResourceGroupClient HttpClient httpClient,
            InternalNodeManager nodeManager,
            JsonCodec<SubmitQueryRequest> submitQueryRequestCodec,
            JsonCodec<QueryResourceGroupState> queryResourceGroupStateCodec)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.currentNodeId = nodeManager.getCurrentNode().getNodeIdentifier();
        this.submitQueryRequestCodec = requireNonNull(submitQueryRequestCodec, "submitQueryRequestCodec is null");
        this.queryResourceGroupStateCodec = requireNonNull(queryResourceGroupStateCodec, "queryResourceGroupStateCodec is null");
    }

    public void submit(QueryId queryId, int queryPriority, SelectionCriteria selectionCriteria, Session session)
    {
        SubmitQueryRequest requestBody = new SubmitQueryRequest(currentNodeId, queryId, queryPriority, selectionCriteria, session.toSessionRepresentation());
        Request request = preparePost()
                .setUri(uriBuilderFrom(nodeManager.getPrimaryCoordinator().getInternalUri())
                        .appendPath("/v1/resourceGroups/submit")
                        .build())
                .addHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .setBodyGenerator(jsonBodyGenerator(submitQueryRequestCodec, requestBody))
                .build();

        httpClient.execute(request, checkResponseStatusCode());
    }

    public void queryStateChanged(ManagedQueryExecution query)
    {
        QueryResourceGroupState requestBody = QueryResourceGroupState.fromQuery(query);
        Request request = preparePost()
                .setUri(uriBuilderFrom(nodeManager.getPrimaryCoordinator().getInternalUri())
                        .appendPath("/v1/resourceGroups/queryStateChanged")
                        .build())
                .addHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .setBodyGenerator(jsonBodyGenerator(queryResourceGroupStateCodec, requestBody))
                .build();

        httpClient.execute(request, checkResponseStatusCode());
    }
}
