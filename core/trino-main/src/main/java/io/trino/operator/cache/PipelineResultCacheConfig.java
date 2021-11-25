package io.trino.operator.cache;

import io.airlift.configuration.Config;

public class PipelineResultCacheConfig
{
    private boolean pipelineResultCacheEnabled = true;

    @Config("pipeline.result.cache.enabled")
    public PipelineResultCacheConfig setPipelineResultCacheEnabled(boolean pipelineResultCacheEnabled)
    {
        this.pipelineResultCacheEnabled = pipelineResultCacheEnabled;
        return this;
    }

    public boolean isPipelineResultCacheEnabled()
    {
        return pipelineResultCacheEnabled;
    }
}
