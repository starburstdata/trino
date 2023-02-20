package io.trino.cost;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.planprinter.PlanNodeStats;

import java.util.Optional;
import java.util.OptionalLong;

public class CachedStatsRule
{
    private final Cache<PlanNodeWrapper, Long> cache;

    public CachedStatsRule()
    {
        cache = CacheBuilder.newBuilder().maximumSize(10_000).build();
    }

    // Returns estimated output row count.
    public Optional<Long> getOutputRowCount(PlanNode node)
    {
        return PlanNodeWrapper.wrap(node).map(cache::getIfPresent);
    }

    public void addStats(PlanNode planNode, PlanNodeStats stats)
    {
        PlanNodeWrapper.wrap(planNode).ifPresent(key -> {
            cache.put(key, stats.getPlanNodeOutputPositions());
        });
    }

    private static class PlanNodeWrapper
    {
        public static Optional<PlanNodeWrapper> wrap(PlanNode node)
        {
            return Optional.empty();
        }
    }
}
