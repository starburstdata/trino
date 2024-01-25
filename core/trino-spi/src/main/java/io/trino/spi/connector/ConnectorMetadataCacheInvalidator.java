package io.trino.spi.connector;

public interface ConnectorMetadataCacheInvalidator
{
    void invalidateTable(SchemaTableName table);
}
