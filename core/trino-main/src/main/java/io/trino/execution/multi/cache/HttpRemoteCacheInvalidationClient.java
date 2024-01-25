package io.trino.execution.multi.cache;

import com.google.common.util.concurrent.Futures;
import com.google.inject.Inject;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.trino.metadata.InternalNode;
import io.trino.metadata.InternalNodeManager;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.multi.RemoteCacheInvalidationClient;

import java.util.List;
import java.util.concurrent.Future;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.trino.execution.multi.resourcegroups.StatusCodeCheckResponseHandler.checkResponseStatusCode;
import static java.util.Objects.requireNonNull;

public class HttpRemoteCacheInvalidationClient
        implements RemoteCacheInvalidationClient
{
    private final InternalNodeManager nodeManager;
    private final HttpClient httpClient;
    private final JsonCodec<CatalogSchemaTableName> catalogSchemaTableNameCodec;
    private final String currentNodeId;

    @Inject
    public HttpRemoteCacheInvalidationClient(
            InternalNodeManager nodeManager,
            @ForRemoteCacheInvalidation HttpClient httpClient,
            JsonCodec<CatalogSchemaTableName> catalogSchemaTableNameCodec)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.catalogSchemaTableNameCodec = requireNonNull(catalogSchemaTableNameCodec, "catalogSchemaTableNameCodec is null");
        this.currentNodeId = nodeManager.getCurrentNode().getNodeIdentifier();
    }

    @Override
    public void invalidateTable(CatalogSchemaTableName table)
    {
        List<Future<Void>> futures = nodeManager.getCoordinators()
                .stream()
                .filter(node -> !node.getNodeIdentifier().equals(currentNodeId))
                .map(coordinator -> invalidateTable(coordinator, table))
                .collect(toImmutableList());
        // Wait for all to finish. Throw if any request fails
        futures.forEach(Futures::getUnchecked);
    }

    private Future<Void> invalidateTable(InternalNode coordinator, CatalogSchemaTableName table)
    {
        Request request = preparePost()
                .setUri(uriBuilderFrom(coordinator.getInternalUri())
                        .appendPath("/v1/cache/invalidate/metadata/table")
                        .build())
                .addHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .setBodyGenerator(jsonBodyGenerator(catalogSchemaTableNameCodec, table))
                .build();

        return httpClient.executeAsync(request, checkResponseStatusCode());
    }
}
