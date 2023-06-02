package io.trino.cost;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.cost.PlanNodeStatsEstimate.EstimateConfidence;
import io.trino.cost.PlanNodeStatsEstimate.RowCountEstimate;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryState;
import io.trino.execution.StageInfo;
import io.trino.sql.planner.Partitioning;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AssignUniqueId;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.MarkDistinctNode;
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
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.SystemSessionProperties.isQueryStatsCacheEnabled;
import static io.trino.execution.StageInfo.getAllStages;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.sql.planner.planprinter.PlanNodeStatsSummarizer.aggregateStageStats;

public class CachedStatsRule
{
    private static final Logger log = Logger.get(CachedStatsRule.class);
    // row count per sub plan per filters
    private final Cache<PlanNodeWrapper, Map<Map<PlanNodeWrapper, List<Expression>>, Long>> cache;

    public CachedStatsRule()
    {
        cache = CacheBuilder.newBuilder().maximumSize(10_000).build();
    }

    // Returns estimated output row count.
    public Optional<RowCountEstimate> getOutputRowCount(PlanNode node, Lookup lookup, Session session)
    {
        if (!isQueryStatsCacheEnabled(session)) {
            return Optional.empty();
        }
        return PlanNodeWrapper.wrap(node, lookup).flatMap(signature -> {
            Map<Map<PlanNodeWrapper, List<Expression>>, Long> rowCountsPerFilters = cache.getIfPresent(signature.root);
            if (rowCountsPerFilters == null) {
                return Optional.empty();
            }

            Long rowCount = rowCountsPerFilters.get(signature.filters());
            if (rowCount != null) {
                // exact match found
                return Optional.of(new RowCountEstimate(rowCount, EstimateConfidence.HIGH));
            }
            // TODO lysy: find best matching filter if any
            return Optional.empty();
        });
    }

    public void queryFinished(QueryInfo finalQueryInfo)
    {
        if (!finalQueryInfo.getState().equals(QueryState.FINISHED)) {
            // ignore failed queries
            return;
        }

        finalQueryInfo.getOutputStage().ifPresent(outputStage -> {
            if (outputStage.getPlan() != null) {
                log.info("caching stats for " + finalQueryInfo);
                List<StageInfo> allStages = getAllStages(finalQueryInfo.getOutputStage());
                Map<PlanNodeId, PlanNodeStats> nodeStats = aggregateStageStats(allStages);
                Map<PlanFragmentId, PlanFragment> fragments = allStages.stream().collect(toImmutableMap(stageInfo -> stageInfo.getPlan().getId(), StageInfo::getPlan));
                PlanNode root = joinFragments(outputStage.getPlan(), fragments);
                addStatsRecursively(root, nodeStats);
            }
        });
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
                return new ExchangeNode(
                        node.getId(),
                        node.getExchangeType(),
                        ExchangeNode.Scope.REMOTE,
                        partitioningScheme,
                        sources,
                        sources.stream().map(PlanNode::getOutputSymbols).collect(toImmutableList()),
                        Optional.empty());
            }
        }, fragment.getRoot());
    }

    private void addStats(PlanNode planNode, PlanNodeStats stats)
    {
        if (stats == null) {
            return;
        }
        PlanNodeWrapper.wrap(planNode, Lookup.noLookup()).ifPresent(signature -> {
            cache.asMap().compute(signature.root, (k, filtersToRowCount) -> {
                if (filtersToRowCount == null) {
                    filtersToRowCount = new ConcurrentHashMap<>();
                }
                filtersToRowCount.put(signature.filters(), stats.getPlanNodeOutputPositions());
                return filtersToRowCount;
            });
        });
    }

    public void flushCache()
    {
        log.info("flushing cache");
        cache.invalidateAll();
    }

    private interface PlanNodeWrapper
    {
        static Optional<RowCountPlanSignature> wrap(PlanNode node, Lookup lookup)
        {
            node = lookup.resolve(node);
            List<RowCountPlanSignature> sources = new ArrayList<>(node.getSources().size());
            for (PlanNode source : node.getSources()) {
                Optional<RowCountPlanSignature> wrappedSource = wrap(source, lookup);
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
            if (node instanceof MarkDistinctNode) {
                // ignore mark distinct
                return Optional.of(sources.get(0));
            }
            if (node instanceof AssignUniqueId) {
                return Optional.of(sources.get(0));
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
            public static Optional<RowCountPlanSignature> wrap(List<RowCountPlanSignature> sources)
            {
                return Optional.of(new RowCountPlanSignature(
                        new Union(sources.stream().map(RowCountPlanSignature::root).collect(toImmutableList())),
                        mergeSubPlanFilters(sources),
                        sources.stream().flatMap(source -> source.rootFilters.stream()).collect(toImmutableList())));
            }

            private static Map<PlanNodeWrapper, List<Expression>> mergeSubPlanFilters(List<RowCountPlanSignature> sources)
            {
                ImmutableMap.Builder<PlanNodeWrapper, List<Expression>> result = ImmutableMap.builder();
                for (RowCountPlanSignature source : sources) {
                    result.putAll(source.subPlanFilters);
                }
                return result.build();
            }
        }

        record Join(JoinNode.Type type, List<JoinNode.EquiJoinClause> criteria, Optional<Expression> filter, PlanNodeWrapper left, PlanNodeWrapper righ)
                implements PlanNodeWrapper
        {
            public static Optional<RowCountPlanSignature> wrap(JoinNode node, RowCountPlanSignature left, RowCountPlanSignature right)
            {
                return Optional.of(new RowCountPlanSignature(
                        new Join(node.getType(), node.getCriteria(), node.getFilter(), left.root, right.root),
                        ImmutableMap.<PlanNodeWrapper, List<Expression>>builder().putAll(left.subPlanFilters).putAll(right.subPlanFilters).build(),
                        ImmutableList.<Expression>builder().addAll(left.rootFilters).addAll(right.rootFilters).build()));
            }
        }

        record Aggregation(List<Symbol> groupingKeys, Set<Integer> globalGroupingSets, PlanNodeWrapper source)
                implements PlanNodeWrapper
        {
            public static Optional<RowCountPlanSignature> wrap(AggregationNode node, RowCountPlanSignature source)
            {
                return Optional.of(new RowCountPlanSignature(
                        new Aggregation(node.getGroupingKeys(), node.getGlobalGroupingSets(), source.root),
                        mergeIfNecessary(source.subPlanFilters, source.root, source.rootFilters),
                        ImmutableList.of()));
            }
        }

        record Filter(Expression predicate, PlanNodeWrapper source)
                implements PlanNodeWrapper
        {
            public static Optional<RowCountPlanSignature> wrap(FilterNode filterNode, RowCountPlanSignature source)
            {
                // TODO lysy: ultimetelly the predicate needs to be decoupled from symbols and based on column references
                return Optional.of(new RowCountPlanSignature(
                        source.root,
                        source.subPlanFilters,
                        ImmutableList.<Expression>builder().addAll(source.rootFilters).add(filterNode.getPredicate()).build()));
            }
        }

        // tableId normally is schema.table
        record TableScan(String catalogId, Object tableId)
                implements PlanNodeWrapper
        {
            public static Optional<RowCountPlanSignature> wrap(TableScanNode tableScan)
            {
                try {
                    return Optional.of(new RowCountPlanSignature(
                            new TableScan(
                                    tableScan.getTable().getCatalogHandle().getId(),
                                    tableScan.getTable().getConnectorHandle().getTableSignatureId()),
                            ImmutableMap.of(),
                            ImmutableList.of())); // TODO lysy: get filters pushed down to table scan
                }
                catch (UnsupportedOperationException e) {
                    return Optional.empty();
                }
            }
        }
    }

    record RowCountPlanSignature(PlanNodeWrapper root, Map<PlanNodeWrapper, List<Expression>> subPlanFilters, List<Expression> rootFilters)
    {
        public Map<PlanNodeWrapper, List<Expression>> filters()
        {
            return mergeIfNecessary(subPlanFilters, root, rootFilters);
        }
    }

    private static Map<PlanNodeWrapper, List<Expression>> mergeIfNecessary(
            Map<PlanNodeWrapper, List<Expression>> subPlanFilters,
            PlanNodeWrapper root,
            List<Expression> rootFilters)
    {
        if (rootFilters.isEmpty()) {
            return subPlanFilters;
        }

        if (subPlanFilters.isEmpty()) {
            return ImmutableMap.of(root, rootFilters);
        }

        return ImmutableMap.<PlanNodeWrapper, List<Expression>>builder()
                .putAll(subPlanFilters)
                .put(root, rootFilters)
                .build();
    }
}
