package io.trino.cost;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.planprinter.PlanNodeStats;
import io.trino.sql.tree.Expression;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

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

    private interface PlanNodeWrapper
    {
        static Optional<PlanNodeWrapper> wrap(PlanNode node)
        {
            List<PlanNodeWrapper> sources = new ArrayList<>();
            for (PlanNode source : node.getSources()) {
                Optional<PlanNodeWrapper> wrappedSource = wrap(source);
                if (wrappedSource.isEmpty()) {
                    // unsupported source
                    return Optional.empty();
                }
                sources.add(wrappedSource.get());
            }
            if (node instanceof FilterNode filterNode) {
                return Filter.wrap(filterNode, sources.get(0));
            }
            if (node instanceof TableScanNode tableScan) {
                return TableScan.wrap(tableScan);
            }
            return Optional.empty();
        }

        record Filter(Expression predicate, PlanNodeWrapper source)
                implements PlanNodeWrapper
        {
            public static Optional<PlanNodeWrapper> wrap(FilterNode filterNode, PlanNodeWrapper source)
            {
                // TODO lysy: ultimetelly the predicate needs to be decoupled from symbols and based on column references
                return Optional.of(new Filter(filterNode.getPredicate(), source));
            }
        }

        // tableId normally is schema.table
        record TableScan(String catalogId, Object tableId)
                implements PlanNodeWrapper
        {
            public static Optional<PlanNodeWrapper> wrap(TableScanNode tableScan)
            {
                return Optional.of(new TableScan(
                        tableScan.getTable().getCatalogHandle().getId(),
                        tableScan.getTable().getConnectorHandle().getTableId()));
            }
        }
    }

    // TODO lysy: stats cache:
    // - addStats from finished queries
    // planNodeWrapper for basic nodes
}
