package io.trino.operator.cache;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;

@Immutable
public class CacheStatsDto
{
    public static final CacheStatsDto ZERO = new CacheStatsDto(0, 0);

    private final long cacheHits;
    private final long cacheMisses;

    @JsonCreator
    public CacheStatsDto(
            @JsonProperty("cacheHit") long cacheHits,
            @JsonProperty("cacheMiss") long cacheMisses)
    {
        this.cacheHits = cacheHits;
        this.cacheMisses = cacheMisses;
    }

    @JsonProperty
    public long getCacheHits()
    {
        return cacheHits;
    }

    @JsonProperty
    public long getCacheMisses()
    {
        return cacheMisses;
    }

    public CacheStatsDto add(CacheStatsDto other)
    {
        return new CacheStatsDto(cacheHits + other.getCacheHits(), cacheMisses + other.getCacheMisses());
    }

    public boolean isZero()
    {
        return cacheHits == 0 && cacheMisses == 0;
    }
}
