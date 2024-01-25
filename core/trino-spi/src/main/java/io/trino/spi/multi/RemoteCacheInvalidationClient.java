package io.trino.spi.multi;

import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.security.TrinoPrincipal;

import java.util.Optional;

public interface RemoteCacheInvalidationClient
{
    void invalidateTable(CatalogSchemaTableName table);

    void invalidateSchema(CatalogSchemaName catalogSchemaName);

    void invalidatePartition(CatalogSchemaTableName catalogSchemaTableName, Optional<String> partitionPredicate);

    void invalidateTablePrivilege(CatalogSchemaTableName catalogSchemaTableName, String tableOwner, TrinoPrincipal grantee);

    void invalidateAll(String catalogName);

    void invalidateAllSchemas(String catalogName);

    static RemoteCacheInvalidationClient noOp()
    {
        return new RemoteCacheInvalidationClient()
        {
            @Override
            public void invalidateTable(CatalogSchemaTableName table)
            {
            }

            @Override
            public void invalidateSchema(CatalogSchemaName catalogSchemaName)
            {
            }

            @Override
            public void invalidatePartition(CatalogSchemaTableName catalogSchemaTableName, Optional<String> partitionPredicate)
            {
            }

            @Override
            public void invalidateTablePrivilege(CatalogSchemaTableName catalogSchemaTableName, String tableOwner, TrinoPrincipal grantee)
            {
            }

            @Override
            public void invalidateAll(String catalogName)
            {
            }

            @Override
            public void invalidateAllSchemas(String catalogName)
            {
            }
        };
    }
}
