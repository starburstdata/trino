package io.trino.operator.cache;

import com.google.inject.Binder;
import com.google.inject.Module;
import io.trino.SystemSessionPropertiesProvider;

import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class PipelineResultCacheModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(PipelineResultCacheConfig.class);
        newSetBinder(binder, SystemSessionPropertiesProvider.class).addBinding()
                .to(PipelineResultCacheSessionProperties.class).in(SINGLETON);
    }
}
