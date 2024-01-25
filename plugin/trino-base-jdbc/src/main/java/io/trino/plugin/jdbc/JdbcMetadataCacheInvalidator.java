package io.trino.plugin.jdbc;

import com.google.inject.Inject;
import io.trino.spi.connector.ConnectorMetadataCacheInvalidator;
import io.trino.spi.connector.SchemaTableName;

import static java.util.Objects.requireNonNull;

public class JdbcMetadataCacheInvalidator
        implements ConnectorMetadataCacheInvalidator
{
    private final CachingJdbcClient cache;

    @Inject
    public JdbcMetadataCacheInvalidator(CachingJdbcClient cache)
    {
        this.cache = requireNonNull(cache, "cache is null");
    }

    @Override
    public void invalidateTable(SchemaTableName table)
    {
        cache.invalidateTableCaches(table, false);
    }

    @Override
    public void invalidateSchemas()
    {
        cache.invalidateSchemasCache(false);
    }

    @Override
    public void invalidateAll()
    {
        cache.flushCache(false);
    }
}
