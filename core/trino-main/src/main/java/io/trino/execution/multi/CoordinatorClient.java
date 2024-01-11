package io.trino.execution.multi;

import com.google.common.util.concurrent.Futures;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClient.HttpResponseFuture;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.trino.metadata.InternalNode;
import io.trino.server.BasicQueryInfo;
import jakarta.inject.Inject;

import java.util.List;
import java.util.concurrent.Future;

import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static java.util.Objects.requireNonNull;

public class CoordinatorClient
{
    private final HttpClient httpClient;
    private final JsonCodec<List<BasicQueryInfo>> basicInfoListCodec;

    @Inject
    public CoordinatorClient(@ForCoordinatorClient HttpClient httpClient, JsonCodec<List<BasicQueryInfo>> basicInfoListCodec)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.basicInfoListCodec = requireNonNull(basicInfoListCodec, "basicInfoListCodec is null");
    }

    public Future<List<BasicQueryInfo>> getAllLocalQueryInfo(InternalNode coordinatorNode)
    {
        Request request = prepareGet()
                .setUri(HttpUriBuilder.uriBuilderFrom(coordinatorNode.getInternalUri())
                        .appendPath("/v1/query/local")
                        .build())
                .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())
//                .setHeader(TRINO_CURRENT_VERSION, Long.toString(taskStatus.getVersion()))
//                .setHeader(TRINO_MAX_WAIT, refreshMaxWait.toString())
//                .setSpanBuilder(spanBuilderFactory.get())
                .build();

        HttpResponseFuture<JsonResponse<List<BasicQueryInfo>>> future = httpClient.executeAsync(request, createFullJsonResponseHandler(basicInfoListCodec));
        return Futures.lazyTransform(future, response -> {
            if (response.getStatusCode() == HttpStatus.OK.code() && response.hasValue()) {
                return response.getValue();
            }
            throw new RuntimeException("Failed to get query info from coordinator: " + coordinatorNode + ", response: " + response);
        });
    }
}
