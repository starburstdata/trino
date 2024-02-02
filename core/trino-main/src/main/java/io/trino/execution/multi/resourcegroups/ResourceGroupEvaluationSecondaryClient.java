package io.trino.execution.multi.resourcegroups;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.concurrent.MoreFutures;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.trino.execution.TaskId;
import io.trino.execution.multi.resourcegroups.ResourceGroupEvaluationPrimaryResource.QueryResourceGroupState;
import io.trino.execution.resourcegroups.QueryQueueFullException;
import io.trino.metadata.InternalNode;
import io.trino.spi.QueryId;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.resourcegroups.ResourceGroupId;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.trino.execution.multi.resourcegroups.StatusCodeCheckResponseHandler.checkResponseStatusCode;
import static java.util.Objects.requireNonNull;

public class ResourceGroupEvaluationSecondaryClient
{
    private final HttpClient httpClient;
    private final JsonCodec<FailQueryRequest> failQueryRequestCodec;
    private final JsonCodec<FailTaskRequest> failTaskRequestCodec;
    private final JsonCodec<QueryStateRequest> queryStateRequestCodec;
    private final JsonCodec<QueryStateResponse> queryStateResponseCodec;

    @Inject
    public ResourceGroupEvaluationSecondaryClient(
            @ForRemoteResourceGroupClient HttpClient httpClient,
            JsonCodec<FailQueryRequest> failQueryRequestCodec,
            JsonCodec<FailTaskRequest> failTaskRequestCodec,
            JsonCodec<QueryStateRequest> queryStateRequestCodec,
            JsonCodec<QueryStateResponse> queryStateResponseCodec)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.failQueryRequestCodec = requireNonNull(failQueryRequestCodec, "failQueryRequestCodec is null");
        this.failTaskRequestCodec = requireNonNull(failTaskRequestCodec, "failTaskRequestCodec is null");
        this.queryStateRequestCodec = requireNonNull(queryStateRequestCodec, "queryStateRequestCodec is null");
        this.queryStateResponseCodec = requireNonNull(queryStateResponseCodec, "queryStateResponseCodec is null");
    }

    public void startWaitingForResources(InternalNode coordinator, QueryId queryId)
    {
        Request request = prepareGet()
                .setUri(uriBuilderFrom(coordinator.getInternalUri())
                        .appendPath("/v1/resourceGroups/startQuery")
                        .appendPath(queryId.toString())
                        .build())
                .build();

        httpClient.execute(request, checkResponseStatusCode());
    }

    public void fail(InternalNode coordinator, QueryId queryId, Throwable cause)
    {
        FailQueryRequest requestBody = new FailQueryRequest(queryId, FailureCause.from(cause));
        Request request = preparePost()
                .setUri(uriBuilderFrom(coordinator.getInternalUri())
                        .appendPath("/v1/resourceGroups/fail")
                        .build())
                .addHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .setBodyGenerator(jsonBodyGenerator(failQueryRequestCodec, requestBody))
                .build();

        httpClient.execute(request, checkResponseStatusCode());
    }

    public void failTask(InternalNode coordinator, TaskId taskId, Exception reason)
    {
        FailTaskRequest requestBody = new FailTaskRequest(taskId, FailureCause.from(reason));
        Request request = preparePost()
                .setUri(uriBuilderFrom(coordinator.getInternalUri())
                        .appendPath("/v1/resourceGroups/failTask")
                        .build())
                .addHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .setBodyGenerator(jsonBodyGenerator(failTaskRequestCodec, requestBody))
                .build();

        httpClient.execute(request, checkResponseStatusCode());
    }

    public CompletableFuture<Map<QueryId, RemoteManagedQueryExecution.RemoteQueryState>> getQueryState(
            InternalNode coordinator, Collection<QueryId> queries)
    {
        QueryStateRequest requestBody = new QueryStateRequest(ImmutableList.copyOf(queries));
        Request request = preparePost()
                .setUri(uriBuilderFrom(coordinator.getInternalUri())
                        .appendPath("/v1/resourceGroups/queryState")
                        .build())
                .addHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .setBodyGenerator(jsonBodyGenerator(queryStateRequestCodec, requestBody))
                .build();
        HttpClient.HttpResponseFuture<QueryStateResponse> future = httpClient.executeAsync(
                request,
                createJsonResponseHandler(queryStateResponseCodec, HttpStatus.OK.code()));
        return MoreFutures.toCompletableFuture(future).thenApply(response -> response
                .queries()
                .stream()
                .collect(toImmutableMap(
                        QueryResourceGroupState::queryId,
                        QueryResourceGroupState::toRemoteQueryState)));
    }

    @JsonSerialize
    public record FailQueryRequest(
            QueryId queryId,
            FailureCause cause)
    {
        public FailQueryRequest
        {
            requireNonNull(queryId, "queryId is null");
            requireNonNull(cause, "cause is null");
        }
    }

    @JsonSerialize
    public record FailTaskRequest(
            TaskId taskId,
            FailureCause cause)
    {
        public FailTaskRequest
        {
            requireNonNull(taskId, "taskId is null");
            requireNonNull(cause, "cause is null");
        }
    }

    @JsonSubTypes({
            @JsonSubTypes.Type(value = GenericErrorCause.class),
            @JsonSubTypes.Type(value = QueryQueueFull.class)
    })
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
    public sealed interface FailureCause
            permits GenericErrorCause, QueryQueueFull
    {
        Exception toException();

        static FailureCause from(Throwable cause)
        {
            return cause instanceof QueryQueueFullException queueFullException ? new QueryQueueFull(queueFullException.getResourceGroup()) : new GenericErrorCause(cause.getMessage());
        }
    }

    @JsonSerialize
    public record GenericErrorCause(String message)
            implements FailureCause
    {
        public GenericErrorCause
        {
            requireNonNull(message, "message is null");
        }

        @Override
        public Exception toException()
        {
            return new TrinoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, message);
        }
    }

    @JsonSerialize
    public record QueryQueueFull(ResourceGroupId resourceGroup)
            implements FailureCause
    {
        public QueryQueueFull
        {
            requireNonNull(resourceGroup, "resourceGroup is null");
        }

        @Override
        public Exception toException()
        {
            return new QueryQueueFullException(resourceGroup);
        }
    }

    @JsonSerialize
    public record QueryStateRequest(List<QueryId> queries)
    {
        public QueryStateRequest
        {
            requireNonNull(queries, "queries is null");
        }
    }

    @JsonSerialize
    public record QueryStateResponse(List<QueryResourceGroupState> queries)
    {
        public QueryStateResponse
        {
            requireNonNull(queries, "queries is null");
        }
    }
}
