package io.trino.operator.cache;

import io.airlift.configuration.Config;

public class PipelineResultCacheConfig
{
    private boolean pipelineResultCacheEnabled = true;
    private boolean pipelineResultCacheCompressionEnabled = true;

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

    @Config("pipeline.result.cache.compression.enabled")
    public PipelineResultCacheConfig setPipelineResultCacheCompressionEnabled(boolean pipelineResultCacheCompressionEnabled)
    {
        this.pipelineResultCacheCompressionEnabled = pipelineResultCacheCompressionEnabled;
        return this;
    }

    public boolean isPipelineResultCacheCompressionEnabled()
    {
        return pipelineResultCacheCompressionEnabled;
    }
}
