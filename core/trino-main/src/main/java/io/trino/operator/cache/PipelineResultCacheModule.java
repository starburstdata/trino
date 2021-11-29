package io.trino.operator.cache;

import com.google.inject.Binder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.SystemSessionPropertiesProvider;

import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.trino.operator.cache.PipelineResultCache.createCache;

public class PipelineResultCacheModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        PipelineResultCacheConfig cacheConfig = buildConfigObject(PipelineResultCacheConfig.class);
        newSetBinder(binder, SystemSessionPropertiesProvider.class).addBinding()
                .to(PipelineResultCacheSessionProperties.class).in(SINGLETON);

        binder.bind(PipelineResultCache.class).toInstance(createCache(cacheConfig.isPipelineResultCacheCompressionEnabled()));
    }
}
