package io.trino.execution.multi.cache;

import com.google.inject.Inject;
import io.trino.connector.CatalogServiceProvider;
import io.trino.metadata.Catalog;
import io.trino.metadata.CatalogManager;
import io.trino.server.security.ResourceSecurity;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ConnectorMetadataCacheInvalidator;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;

import java.util.NoSuchElementException;

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
        Catalog catalog = catalogManager.getCatalog(table.getCatalogName())
                .orElseThrow(() -> new NoSuchElementException("catalog not found " + table.getCatalogName()));
        metadataCacheInvalidator.getService(catalog.getCatalogHandle()).invalidateTable(table.getSchemaTableName());
    }
}
