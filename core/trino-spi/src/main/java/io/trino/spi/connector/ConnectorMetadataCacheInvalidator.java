package io.trino.spi.connector;

import io.trino.spi.security.TrinoPrincipal;

import java.util.Optional;

public interface ConnectorMetadataCacheInvalidator
{
    void invalidateTable(SchemaTableName table);

    void invalidateSchema(CatalogSchemaName catalogSchemaName);

    void invalidatePartition(CatalogSchemaTableName table, Optional<String> partitionPredicate);

    void invalidateTablePrivilege(CatalogSchemaTableName table, String tableOwner, TrinoPrincipal grantee);

    void invalidateAll();
}
