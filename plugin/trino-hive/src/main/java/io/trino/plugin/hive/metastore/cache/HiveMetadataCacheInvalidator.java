package io.trino.plugin.hive.metastore.cache;

import com.google.inject.Inject;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.plugin.hive.metastore.cache.SharedHiveMetastoreCache.CachingHiveMetastoreFactory;
import io.trino.plugin.hive.metastore.cache.SharedHiveMetastoreCache.ImpersonationCachingHiveMetastoreFactory;
import io.trino.spi.connector.ConnectorMetadataCacheInvalidator;
import io.trino.spi.connector.SchemaTableName;

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
        if (hiveMetastoreFactory instanceof CachingHiveMetastoreFactory factory) {
            factory.getMetastore().invalidateTable(table.getSchemaName(), table.getTableName(), false);
        }
        else if (hiveMetastoreFactory instanceof ImpersonationCachingHiveMetastoreFactory factory) {
            factory.getAllHiveMetastores().forEach(cachingHiveMetastore ->
                    cachingHiveMetastore.invalidateTable(
                            table.getSchemaName(),
                            table.getTableName(),
                            false));
        }
    }
}
