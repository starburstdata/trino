package io.trino.execution.multi;

import com.google.common.util.concurrent.Futures;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.trino.execution.QueryInfo;
import io.trino.execution.multi.resourcegroups.ResourceGroupEvaluationSecondaryClient;
import io.trino.execution.multi.resourcegroups.ResourceGroupEvaluationSecondaryClient.FailQueryRequest;
import io.trino.metadata.InternalNode;
import io.trino.server.BasicQueryInfo;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import jakarta.inject.Inject;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Future;

import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.trino.execution.multi.resourcegroups.StatusCodeCheckResponseHandler.checkResponseStatusCode;
import static java.util.Objects.requireNonNull;

public class InternalCoordinatorClient
{
    private final HttpClient httpClient;
    private final JsonCodec<List<BasicQueryInfo>> basicInfoListCodec;
    private final JsonCodec<BasicQueryInfo> basicInfoCodec;
    private final JsonCodec<Optional<QueryInfo>> fullQueryInfoCodec;
    private final JsonCodec<IsQueryRegisteredResponse> isQueryRegisteredResponseCodec;
    private final JsonCodec<FailQueryRequest> failQueryRequestCodec;

    @Inject
    public InternalCoordinatorClient(
            @ForCoordinatorClient HttpClient httpClient,
            JsonCodec<List<BasicQueryInfo>> basicInfoListCodec,
            JsonCodec<BasicQueryInfo> basicInfoCodec,
            JsonCodec<Optional<QueryInfo>> fullQueryInfoCodec,
            JsonCodec<IsQueryRegisteredResponse> isQueryRegisteredResponseCodec,
            JsonCodec<FailQueryRequest> failQueryRequestCodec)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.basicInfoListCodec = requireNonNull(basicInfoListCodec, "basicInfoListCodec is null");
        this.basicInfoCodec = requireNonNull(basicInfoCodec, "basicInfoCodec is null");
        this.fullQueryInfoCodec = requireNonNull(fullQueryInfoCodec, "fullQueryInfoCodec is null");
        this.isQueryRegisteredResponseCodec = requireNonNull(isQueryRegisteredResponseCodec, "isQueryRegisteredResponseCodec is null");
        this.failQueryRequestCodec = requireNonNull(failQueryRequestCodec, "failQueryRequestCodec is null");
    }

    public Future<List<BasicQueryInfo>> getAllLocalQueryInfo(InternalNode coordinatorNode)
    {
        Request request = prepareGet()
                .setUri(HttpUriBuilder.uriBuilderFrom(coordinatorNode.getInternalUri())
                        .appendPath("/v1/query/local")
                        .build())
                .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .build();

        return httpClient.executeAsync(request, createJsonResponseHandler(basicInfoListCodec, HttpStatus.OK.code()));
    }

    public Optional<QueryInfo> getFullQueryInfo(InternalNode coordinatorNode, QueryId queryId)
    {
        Request request = prepareGet()
                .setUri(HttpUriBuilder.uriBuilderFrom(coordinatorNode.getInternalUri())
                        .appendPath("/v1/coordinator/internal/query/fullInfo")
                        .appendPath(queryId.toString())
                        .build())
                .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .build();
        return httpClient.execute(request, createJsonResponseHandler(fullQueryInfoCodec, HttpStatus.OK.code()));
    }

    public BasicQueryInfo getQueryInfo(InternalNode coordinatorNode, QueryId queryId)
    {
        Request request = prepareGet()
                .setUri(HttpUriBuilder.uriBuilderFrom(coordinatorNode.getInternalUri())
                        .appendPath("/v1/coordinator/internal/query/basicInfo")
                        .appendPath(queryId.toString())
                        .build())
                .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .build();
        return httpClient.execute(request, createJsonResponseHandler(basicInfoCodec, HttpStatus.OK.code()));
    }

    public Future<Boolean> isQueryRegistered(InternalNode coordinatorNode, QueryId queryId)
    {
        Request request = prepareGet()
                .setUri(HttpUriBuilder.uriBuilderFrom(coordinatorNode.getInternalUri())
                        .appendPath("/v1/coordinator/internal/query/registered")
                        .appendPath(queryId.toString())
                        .build())
                .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .build();
        return Futures.lazyTransform(
                httpClient.executeAsync(request, createJsonResponseHandler(isQueryRegisteredResponseCodec, HttpStatus.OK.code())),
                IsQueryRegisteredResponse::registered);
    }

    public void cancelQuery(InternalNode coordinatorNode, QueryId queryId)
    {
        Request request = preparePost()
                .setUri(uriBuilderFrom(coordinatorNode.getInternalUri())
                        .appendPath("/v1/coordinator/internal/query/cancel")
                        .appendPath(queryId.toString())
                        .build())
                .addHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .build();

        httpClient.execute(request, checkResponseStatusCode());
    }

    public void failQuery(InternalNode coordinatorNode, QueryId queryId, TrinoException cause)
    {
        FailQueryRequest requestBody = new FailQueryRequest(queryId, ResourceGroupEvaluationSecondaryClient.FailureCause.from(cause));
        Request request = preparePost()
                .setUri(uriBuilderFrom(coordinatorNode.getInternalUri())
                        .appendPath("/v1/coordinator/internal/query/fail")
                        .build())
                .addHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .setBodyGenerator(jsonBodyGenerator(failQueryRequestCodec, requestBody))
                .build();

        httpClient.execute(request, checkResponseStatusCode());
    }

    public record IsQueryRegisteredResponse(boolean registered)
    {
    }
}
