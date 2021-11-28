package io.trino.operator.cache;

import java.util.concurrent.atomic.AtomicLong;

public class CacheStats
{
    private final AtomicLong cacheHit = new AtomicLong();
    private final AtomicLong cacheMiss = new AtomicLong();

    public void recordCacheHit()
    {
        cacheHit.incrementAndGet();
    }

    public void recordCacheMiss()
    {
        cacheMiss.incrementAndGet();
    }

    public void update(CacheStatsDto resultCacheStats)
    {
        update(resultCacheStats.getCacheHits(), resultCacheStats.getCacheMisses());
    }

    public void update(CacheStats resultCacheStats)
    {
        update(resultCacheStats.cacheHit.get(), resultCacheStats.cacheMiss.get());
    }

    public void update(long cacheHit, long cacheMiss)
    {
        this.cacheHit.addAndGet(cacheHit);
        this.cacheMiss.addAndGet(cacheMiss);
    }

    public CacheStatsDto toDto()
    {
        return new CacheStatsDto(cacheHit.get(), cacheMiss.get());
    }
}
