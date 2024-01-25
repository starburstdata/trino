package io.trino.execution.multi.cache;

import com.google.common.util.concurrent.Futures;
import com.google.inject.Inject;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.trino.metadata.InternalNode;
import io.trino.metadata.InternalNodeManager;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.multi.RemoteCacheInvalidationClient;
import io.trino.spi.security.TrinoPrincipal;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.trino.execution.multi.cache.MetadataCacheInvalidatorResource.InvalidatePartitionRequest;
import static io.trino.execution.multi.cache.MetadataCacheInvalidatorResource.InvalidateTablePrivilegeRequest;
import static io.trino.execution.multi.resourcegroups.StatusCodeCheckResponseHandler.checkResponseStatusCode;
import static java.util.Objects.requireNonNull;

public class HttpRemoteCacheInvalidationClient
        implements RemoteCacheInvalidationClient
{
    private final InternalNodeManager nodeManager;
    private final HttpClient httpClient;
    private final JsonCodec<CatalogSchemaTableName> catalogSchemaTableNameCodec;
    private final JsonCodec<CatalogSchemaName> catalogSchemaNameCodec;
    private final JsonCodec<InvalidatePartitionRequest> invalidatePartitionRequestCodec;
    private final JsonCodec<InvalidateTablePrivilegeRequest> invalidateTablePrivilegeRequestCodec;
    private final String currentNodeId;

    @Inject
    public HttpRemoteCacheInvalidationClient(
            InternalNodeManager nodeManager,
            @ForRemoteCacheInvalidation HttpClient httpClient,
            JsonCodec<CatalogSchemaTableName> catalogSchemaTableNameCodec,
            JsonCodec<CatalogSchemaName> catalogSchemaNameCodec,
            JsonCodec<InvalidatePartitionRequest> invalidatePartitionRequestCodec,
            JsonCodec<InvalidateTablePrivilegeRequest> invalidateTablePrivilegeRequestCodec)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.catalogSchemaTableNameCodec = requireNonNull(catalogSchemaTableNameCodec, "catalogSchemaTableNameCodec is null");
        this.currentNodeId = nodeManager.getCurrentNode().getNodeIdentifier();
        this.catalogSchemaNameCodec = catalogSchemaNameCodec;
        this.invalidatePartitionRequestCodec = invalidatePartitionRequestCodec;
        this.invalidateTablePrivilegeRequestCodec = invalidateTablePrivilegeRequestCodec;
    }

    @Override
    public void invalidateTable(CatalogSchemaTableName table)
    {
        invalidateOnCoordinators(coordinator -> invalidateTable(coordinator, table));
    }

    @Override
    public void invalidateSchema(CatalogSchemaName catalogSchemaName)
    {
        invalidateOnCoordinators(coordinator -> invalidateDatabase(coordinator, catalogSchemaName));
    }

    @Override
    public void invalidatePartition(CatalogSchemaTableName catalogSchemaTableName, Optional<String> partitionPredicate)
    {
        invalidateOnCoordinators(coordinator -> invalidatePartition(coordinator, catalogSchemaTableName, partitionPredicate));
    }

    @Override
    public void invalidateTablePrivilege(CatalogSchemaTableName table, String tableOwner, TrinoPrincipal grantee)
    {
        invalidateOnCoordinators(coordinator -> invalidateTablePrivilege(coordinator, table, tableOwner, grantee));
    }

    @Override
    public void invalidateAll(String catalogName)
    {
        invalidateOnCoordinators(coordinator -> invalidateAll(coordinator, catalogName));
    }

    @Override
    public void invalidateAllSchemas(String catalogName)
    {
        invalidateOnCoordinators(coordinator -> invalidateAllSchemas(coordinator, catalogName));
    }

    private void invalidateOnCoordinators(Function<InternalNode, Future<Void>> invalidatorCall)
    {
        List<Future<Void>> futures = nodeManager.getCoordinators()
                .stream()
                .filter(node -> !node.getNodeIdentifier().equals(currentNodeId))
                .map(invalidatorCall::apply)
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

    private Future<Void> invalidateDatabase(InternalNode coordinator, CatalogSchemaName catalogSchemaName)
    {
        Request request = preparePost()
                .setUri(uriBuilderFrom(coordinator.getInternalUri())
                        .appendPath("/v1/cache/invalidate/metadata/schema")
                        .build())
                .addHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .setBodyGenerator(jsonBodyGenerator(catalogSchemaNameCodec, catalogSchemaName))
                .build();

        return httpClient.executeAsync(request, checkResponseStatusCode());
    }

    private Future<Void> invalidatePartition(InternalNode coordinator, CatalogSchemaTableName table, Optional<String> partitionPredicate)
    {
        Request request = preparePost()
                .setUri(uriBuilderFrom(coordinator.getInternalUri())
                        .appendPath("/v1/cache/invalidate/metadata/partition")
                        .build())
                .addHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .setBodyGenerator(jsonBodyGenerator(invalidatePartitionRequestCodec, new InvalidatePartitionRequest(table, partitionPredicate)))
                .build();

        return httpClient.executeAsync(request, checkResponseStatusCode());
    }

    private Future<Void> invalidateTablePrivilege(InternalNode coordinator, CatalogSchemaTableName table, String tableOwner, TrinoPrincipal grantee)
    {
        Request request = preparePost()
                .setUri(uriBuilderFrom(coordinator.getInternalUri())
                        .appendPath("/v1/cache/invalidate/metadata/tablePrivilege")
                        .build())
                .addHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .setBodyGenerator(jsonBodyGenerator(invalidateTablePrivilegeRequestCodec, new InvalidateTablePrivilegeRequest(table, tableOwner, grantee)))
                .build();

        return httpClient.executeAsync(request, checkResponseStatusCode());
    }

    private Future<Void> invalidateAll(InternalNode coordinator, String catalogName)
    {
        Request request = preparePost()
                .setUri(uriBuilderFrom(coordinator.getInternalUri())
                        .appendPath("/v1/cache/invalidate/metadata/all")
                        .appendPath(catalogName)
                        .build())
                .addHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .build();

        return httpClient.executeAsync(request, checkResponseStatusCode());
    }

    private Future<Void> invalidateAllSchemas(InternalNode coordinator, String catalogName)
    {
        Request request = preparePost()
                .setUri(uriBuilderFrom(coordinator.getInternalUri())
                        .appendPath("/v1/cache/invalidate/metadata/schemas")
                        .appendPath(catalogName)
                        .build())
                .addHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .build();

        return httpClient.executeAsync(request, checkResponseStatusCode());
    }
}
