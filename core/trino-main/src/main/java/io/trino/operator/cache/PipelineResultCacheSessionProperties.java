package io.trino.operator.cache;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.SystemSessionPropertiesProvider;
import io.trino.spi.session.PropertyMetadata;

import javax.inject.Inject;

import java.util.List;

import static io.trino.spi.session.PropertyMetadata.booleanProperty;

public class PipelineResultCacheSessionProperties
        implements SystemSessionPropertiesProvider
{
    public static final String PIPELINE_RESULT_CACHE_ENABLED = "pipeline_result_cache_enabled";

    private final List<PropertyMetadata<?>> properties;

    @Inject
    public PipelineResultCacheSessionProperties(PipelineResultCacheConfig config)
    {
        properties = ImmutableList.of(
                booleanProperty(
                        PIPELINE_RESULT_CACHE_ENABLED,
                        "Enable pipeline result cache",
                        config.isPipelineResultCacheEnabled(),
                        false));
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return properties;
    }

    public static boolean isPipelineResultCacheEnabled(Session session)
    {
        return session.getSystemProperty(PIPELINE_RESULT_CACHE_ENABLED, Boolean.class);
    }
}
