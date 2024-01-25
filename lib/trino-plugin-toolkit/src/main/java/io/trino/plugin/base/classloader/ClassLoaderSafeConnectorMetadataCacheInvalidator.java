package io.trino.plugin.base.classloader;

import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ConnectorMetadataCacheInvalidator;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.TrinoPrincipal;

import java.util.Optional;

public class ClassLoaderSafeConnectorMetadataCacheInvalidator
        implements ConnectorMetadataCacheInvalidator
{
    private final ConnectorMetadataCacheInvalidator delegate;
    private final ClassLoader classLoader;

    public ClassLoaderSafeConnectorMetadataCacheInvalidator(ConnectorMetadataCacheInvalidator delegate, ClassLoader classLoader)
    {
        this.delegate = delegate;
        this.classLoader = classLoader;
    }

    @Override
    public void invalidateTable(SchemaTableName table)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.invalidateTable(table);
        }
    }

    @Override
    public void invalidateSchema(CatalogSchemaName catalogSchemaName)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.invalidateSchema(catalogSchemaName);
        }
    }

    @Override
    public void invalidatePartition(CatalogSchemaTableName table, Optional<String> partitionPredicate)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.invalidatePartition(table, partitionPredicate);
        }
    }

    @Override
    public void invalidateTablePrivilege(CatalogSchemaTableName table, String tableOwner, TrinoPrincipal grantee)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.invalidateTablePrivilege(table, tableOwner, grantee);
        }
    }

    @Override
    public void invalidateAll()
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.invalidateAll();
        }
    }
}
