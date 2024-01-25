package io.trino.execution.multi.cache;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.execution.multi.cache.MetadataCacheInvalidatorResource.InvalidatePartitionRequest;
import io.trino.execution.multi.cache.MetadataCacheInvalidatorResource.InvalidateTablePrivilegeRequest;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.multi.RemoteCacheInvalidationClient;

import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static io.trino.server.InternalCommunicationHttpClientModule.internalHttpClientModule;

public class RemoteCacheInvalidatorModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        jsonCodecBinder(binder).bindJsonCodec(CatalogSchemaTableName.class);
        jsonCodecBinder(binder).bindJsonCodec(CatalogSchemaName.class);
        jsonCodecBinder(binder).bindJsonCodec(InvalidatePartitionRequest.class);
        jsonCodecBinder(binder).bindJsonCodec(InvalidateTablePrivilegeRequest.class);
        install(internalHttpClientModule("remote-cache-invalidation", ForRemoteCacheInvalidation.class)
                .withTracing()
                .build());
        binder.bind(RemoteCacheInvalidationClient.class).to(HttpRemoteCacheInvalidationClient.class).in(Scopes.SINGLETON);
        jaxrsBinder(binder).bind(MetadataCacheInvalidatorResource.class);
    }
}
