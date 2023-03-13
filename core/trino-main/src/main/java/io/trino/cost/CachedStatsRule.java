package io.trino.cost;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.trino.execution.QueryInfo;
import io.trino.execution.StageInfo;
import io.trino.sql.planner.Partitioning;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.RemoteSourceNode;
import io.trino.sql.planner.plan.SimplePlanRewriter;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.UnionNode;
import io.trino.sql.planner.planprinter.PlanNodeStats;
import io.trino.sql.tree.Expression;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.execution.StageInfo.getAllStages;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.sql.planner.planprinter.PlanNodeStatsSummarizer.aggregateStageStats;

public class CachedStatsRule
{
    private static final Logger log = Logger.get(CachedStatsRule.class);
    private final Cache<PlanNodeWrapper, Long> cache;

    public CachedStatsRule()
    {
        cache = CacheBuilder.newBuilder().maximumSize(10_000).build();
    }

    // Returns estimated output row count.
    public Optional<Long> getOutputRowCount(PlanNode node, Lookup lookup)
    {
        return PlanNodeWrapper.wrap(node, lookup).map(cache::getIfPresent);
    }

    public void queryFinished(QueryInfo finalQueryInfo)
    {
        log.info("caching stats for " + finalQueryInfo);
        // TODO lysy join plan fragments roots to one root plan

        List<StageInfo> allStages = getAllStages(finalQueryInfo.getOutputStage());
        Map<PlanNodeId, PlanNodeStats> nodeStats = aggregateStageStats(allStages);
        Map<PlanFragmentId, PlanFragment> fragments = allStages.stream().collect(toImmutableMap(stageInfo -> stageInfo.getPlan().getId(), StageInfo::getPlan));
        PlanNode root = joinFragments(finalQueryInfo.getOutputStage().get().getPlan(), fragments);
        addStatsRecursively(root, nodeStats);
    }

    private void addStatsRecursively(PlanNode node, Map<PlanNodeId, PlanNodeStats> nodeStats)
    {
        addStats(node, nodeStats.get(node.getId()));
        node.getSources().forEach(source -> addStatsRecursively(source, nodeStats));
    }

    private PlanNode joinFragments(PlanFragment fragment, Map<PlanFragmentId, PlanFragment> allFragments)
    {
        return SimplePlanRewriter.rewriteWith(new SimplePlanRewriter<>()
        {
            @Override
            public PlanNode visitRemoteSource(RemoteSourceNode node, RewriteContext<Object> context)
            {
                List<PlanNode> sources = node.getSourceFragmentIds().stream().map(fragmentId -> joinFragments(allFragments.get(fragmentId), allFragments)).collect(toImmutableList());

                PartitioningScheme partitioningScheme = new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), node.getOutputSymbols());
                return new ExchangeNode(node.getId(), node.getExchangeType(), ExchangeNode.Scope.REMOTE, partitioningScheme, sources,
                        ImmutableList.of(partitioningScheme.getOutputLayout()), Optional.empty());
            }
        }, fragment.getRoot());
    }

    private void addStats(PlanNode planNode, PlanNodeStats stats)
    {
        PlanNodeWrapper.wrap(planNode, Lookup.noLookup()).ifPresent(key -> {
            cache.put(key, stats.getPlanNodeOutputPositions());
        });
    }

    private interface PlanNodeWrapper
    {
        static Optional<PlanNodeWrapper> wrap(PlanNode node, Lookup lookup)
        {
            node = lookup.resolve(node);
            List<PlanNodeWrapper> sources = new ArrayList<>(node.getSources().size());
            for (PlanNode source : node.getSources()) {
                Optional<PlanNodeWrapper> wrappedSource = wrap(source, lookup);
                if (wrappedSource.isEmpty()) {
                    // unsupported source
                    return Optional.empty();
                }
                sources.add(wrappedSource.get());
            }
            if (node instanceof JoinNode joinNode) {
                return Join.wrap(joinNode, sources.get(0), sources.get(1));
            }
            if (node instanceof AggregationNode aggregationNode) {
                return Aggregation.wrap(aggregationNode, sources.get(0));
            }
            if (node instanceof ProjectNode) {
                // ignore project
                return Optional.of(sources.get(0));
            }
            if (node instanceof ExchangeNode || node instanceof UnionNode) {
                if (node.getSources().size() == 1) {
                    // ignore normal exchange
                    return Optional.of(sources.get(0));
                }
                return Union.wrap(sources);
            }
            if (node instanceof FilterNode filterNode) {
                return Filter.wrap(filterNode, sources.get(0));
            }
            if (node instanceof TableScanNode tableScan) {
                return TableScan.wrap(tableScan);
            }
            return Optional.empty();
        }

        record Union(List<PlanNodeWrapper> sources)
                implements PlanNodeWrapper
        {
            public static Optional<PlanNodeWrapper> wrap(List<PlanNodeWrapper> sources)
            {
                return Optional.of(new Union(sources));
            }
        }

        record Join(JoinNode.Type type, List<JoinNode.EquiJoinClause> criteria, Optional<Expression> filter, PlanNodeWrapper left, PlanNodeWrapper righ)
                implements PlanNodeWrapper
        {
            public static Optional<PlanNodeWrapper> wrap(JoinNode node, PlanNodeWrapper left, PlanNodeWrapper right)
            {
                return Optional.of(new Join(node.getType(), node.getCriteria(), node.getFilter(), left, right));
            }
        }

        record Aggregation(List<Symbol> groupingKeys, Set<Integer> globalGroupingSets, PlanNodeWrapper source)
                implements PlanNodeWrapper
        {
            public static Optional<PlanNodeWrapper> wrap(AggregationNode node, PlanNodeWrapper source)
            {
                return Optional.of(new Aggregation(node.getGroupingKeys(), node.getGlobalGroupingSets(), source));
            }
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
    // planNodeWrapper for basic nodes X
}
