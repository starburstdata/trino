package io.trino.plugin.base.classloader;

import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.ConnectorMetadataCacheInvalidator;
import io.trino.spi.connector.SchemaTableName;

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
}
