package io.trino.execution.multi.cache;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.inject.Inject;
import io.trino.connector.CatalogServiceProvider;
import io.trino.metadata.Catalog;
import io.trino.metadata.CatalogManager;
import io.trino.server.security.ResourceSecurity;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ConnectorMetadataCacheInvalidator;
import io.trino.spi.security.TrinoPrincipal;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;

import java.util.NoSuchElementException;
import java.util.Optional;

import static io.trino.server.security.ResourceSecurity.AccessType.INTERNAL_ONLY;
import static java.util.Objects.requireNonNull;

@Path("/v1/cache/invalidate/metadata")
public class MetadataCacheInvalidatorResource
{
    private final CatalogManager catalogManager;
    private final CatalogServiceProvider<ConnectorMetadataCacheInvalidator> metadataCacheInvalidator;

    @Inject
    public MetadataCacheInvalidatorResource(CatalogManager catalogManager, CatalogServiceProvider<ConnectorMetadataCacheInvalidator> metadataCacheInvalidator)
    {
        this.catalogManager = requireNonNull(catalogManager, "catalogManager is null");
        this.metadataCacheInvalidator = requireNonNull(metadataCacheInvalidator, "metadataCacheInvalidator is null");
    }

    @ResourceSecurity(INTERNAL_ONLY)
    @POST
    @Path("table")
    public void invalidateTable(CatalogSchemaTableName table)
    {
        getInvalidator(table.getCatalogName()).invalidateTable(table.getSchemaTableName());
    }

    @ResourceSecurity(INTERNAL_ONLY)
    @POST
    @Path("schema")
    public void invalidateSchema(CatalogSchemaName catalogSchemaName)
    {
        getInvalidator(catalogSchemaName.getCatalogName()).invalidateSchema(catalogSchemaName);
    }

    @ResourceSecurity(INTERNAL_ONLY)
    @POST
    @Path("partition")
    public void invalidatePartition(InvalidatePartitionRequest request)
    {
        getInvalidator(request.table().getCatalogName()).invalidatePartition(request.table(), request.partitionPredicate());
    }

    @ResourceSecurity(INTERNAL_ONLY)
    @POST
    @Path("tablePrivilege")
    public void invalidateTablePrivilege(InvalidateTablePrivilegeRequest request)
    {
        getInvalidator(request.table().getCatalogName()).invalidateTablePrivilege(request.table(), request.tableOwner(), request.grantee());
    }

    @ResourceSecurity(INTERNAL_ONLY)
    @POST
    @Path("all/{catalogName}")
    public void invalidateAll(@PathParam("catalogName") String catalogName)
    {
        getInvalidator(catalogName).invalidateAll();
    }

    @ResourceSecurity(INTERNAL_ONLY)
    @POST
    @Path("schemas/{catalogName}")
    public void invalidateSchemas(@PathParam("catalogName") String catalogName)
    {
        getInvalidator(catalogName).invalidateSchemas();
    }

    private ConnectorMetadataCacheInvalidator getInvalidator(String catalogName)
    {
        Catalog catalog = catalogManager.getCatalog(catalogName)
                .orElseThrow(() -> new NoSuchElementException("catalog not found " + catalogName));
        return metadataCacheInvalidator.getService(catalog.getCatalogHandle());
    }

    @JsonSerialize
    public record InvalidatePartitionRequest(CatalogSchemaTableName table, Optional<String> partitionPredicate)
    {
    }

    @JsonSerialize
    public record InvalidateTablePrivilegeRequest(CatalogSchemaTableName table, String tableOwner, TrinoPrincipal grantee)
    {
    }
}
