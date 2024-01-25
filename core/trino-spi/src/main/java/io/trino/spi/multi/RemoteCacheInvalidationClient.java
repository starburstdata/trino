package io.trino.spi.multi;

import io.trino.spi.connector.CatalogSchemaTableName;

public interface RemoteCacheInvalidationClient
{
    void invalidateTable(CatalogSchemaTableName table);

    static RemoteCacheInvalidationClient noOp()
    {
        return (table) -> {};
    }
}
