package io.trino.plugin.hive.metastore.cache;

import com.google.inject.Inject;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.plugin.hive.metastore.HivePrincipal;
import io.trino.plugin.hive.metastore.cache.SharedHiveMetastoreCache.CachingHiveMetastoreFactory;
import io.trino.plugin.hive.metastore.cache.SharedHiveMetastoreCache.ImpersonationCachingHiveMetastoreFactory;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ConnectorMetadataCacheInvalidator;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.TrinoPrincipal;

import java.util.Optional;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

public class HiveMetadataCacheInvalidator
        implements ConnectorMetadataCacheInvalidator
{
    private final HiveMetastoreFactory hiveMetastoreFactory;

    @Inject
    public HiveMetadataCacheInvalidator(HiveMetastoreFactory hiveMetastoreFactory)
    {
        this.hiveMetastoreFactory = requireNonNull(hiveMetastoreFactory, "hiveMetastoreFactory is null");
    }

    @Override
    public void invalidateTable(SchemaTableName table)
    {
        invalidate(cachingHiveMetastore -> cachingHiveMetastore.invalidateTable(table.getSchemaName(), table.getTableName(), false));
    }

    @Override
    public void invalidateSchema(CatalogSchemaName catalogSchemaName)
    {
        invalidate(cachingHiveMetastore -> cachingHiveMetastore.invalidateDatabase(catalogSchemaName.getSchemaName(), false));
    }

    @Override
    public void invalidatePartition(CatalogSchemaTableName table, Optional<String> partitionPredicate)
    {
        invalidate(cachingHiveMetastore -> cachingHiveMetastore.invalidatePartitionCache(
                table.getSchemaTableName().getSchemaName(),
                table.getSchemaTableName().getTableName(),
                partitionPredicate,
                false));
    }

    @Override
    public void invalidateTablePrivilege(CatalogSchemaTableName table, String tableOwner, TrinoPrincipal grantee)
    {
        invalidate(cachingHiveMetastore -> cachingHiveMetastore.invalidateTablePrivilegeCacheEntries(
                table.getSchemaTableName().getSchemaName(),
                table.getSchemaTableName().getTableName(),
                tableOwner,
                HivePrincipal.from(grantee),
                false));
    }

    @Override
    public void invalidateAll()
    {
        invalidate(cachingHiveMetastore -> cachingHiveMetastore.flushCache(false));
    }

    private void invalidate(Consumer<CachingHiveMetastore> invalidator)
    {
        if (hiveMetastoreFactory instanceof CachingHiveMetastoreFactory factory) {
            invalidator.accept(factory.getMetastore());
        }
        else if (hiveMetastoreFactory instanceof ImpersonationCachingHiveMetastoreFactory factory) {
            factory.getAllHiveMetastores().forEach(invalidator);
        }
    }
}
