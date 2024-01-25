package io.trino.spi.connector;

import io.trino.spi.security.TrinoPrincipal;

import java.util.Optional;

public interface ConnectorMetadataCacheInvalidator
{
    default void invalidateTable(SchemaTableName table)
    {
        throw new UnsupportedOperationException();
    }

    default void invalidateSchema(CatalogSchemaName catalogSchemaName)
    {
        throw new UnsupportedOperationException();
    }

    default void invalidatePartition(CatalogSchemaTableName table, Optional<String> partitionPredicate)
    {
        throw new UnsupportedOperationException();
    }

    default void invalidateTablePrivilege(CatalogSchemaTableName table, String tableOwner, TrinoPrincipal grantee)
    {
        throw new UnsupportedOperationException();
    }

    void invalidateAll();

    default void invalidateSchemas()
    {
        throw new UnsupportedOperationException();
    }
}
