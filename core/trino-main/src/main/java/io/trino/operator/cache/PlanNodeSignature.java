/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.operator.cache;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.sql.planner.iterative.GroupReference;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.ApplyNode;
import io.trino.sql.planner.plan.AssignUniqueId;
import io.trino.sql.planner.plan.CorrelatedJoinNode;
import io.trino.sql.planner.plan.DeleteNode;
import io.trino.sql.planner.plan.DistinctLimitNode;
import io.trino.sql.planner.plan.EnforceSingleRowNode;
import io.trino.sql.planner.plan.ExceptNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.ExplainAnalyzeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.GroupIdNode;
import io.trino.sql.planner.plan.IndexJoinNode;
import io.trino.sql.planner.plan.IndexSourceNode;
import io.trino.sql.planner.plan.IntersectNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.MarkDistinctNode;
import io.trino.sql.planner.plan.OffsetNode;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.PatternRecognitionNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.RefreshMaterializedViewNode;
import io.trino.sql.planner.plan.RemoteSourceNode;
import io.trino.sql.planner.plan.RowNumberNode;
import io.trino.sql.planner.plan.SampleNode;
import io.trino.sql.planner.plan.SemiJoinNode;
import io.trino.sql.planner.plan.SortNode;
import io.trino.sql.planner.plan.SpatialJoinNode;
import io.trino.sql.planner.plan.StatisticsWriterNode;
import io.trino.sql.planner.plan.TableDeleteNode;
import io.trino.sql.planner.plan.TableExecuteNode;
import io.trino.sql.planner.plan.TableFinishNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TableWriterNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.planner.plan.TopNRankingNode;
import io.trino.sql.planner.plan.UnionNode;
import io.trino.sql.planner.plan.UnnestNode;
import io.trino.sql.planner.plan.UpdateNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.planner.plan.WindowNode;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public interface PlanNodeSignature
{
    Logger log = Logger.get(CachingDriver.class);

    PlanNodeSignature UNKNOWN = () -> false;

    boolean canCache();

    static PlanNodeSignature from(PlanNode plan)
    {
        return plan.accept(new Visitor(), null);
    }

    class Visitor
            extends PlanVisitor<PlanNodeSignature, Void>
    {
        @Override
        protected PlanNodeSignature visitPlan(PlanNode node, Void context)
        {
            log.debug("missing signature for: " + node);
            return new GenericPlanNodeSignature(
                    false,
                    node,
                    node.getSources().stream()
                            .map(source -> source.accept(this, context))
                            .collect(toImmutableList()));
        }

        @Override
        public PlanNodeSignature visitAggregation(AggregationNode node, Void context)
        {
            return signature(
                    node,
                    context,
                    true,
                    ImmutableMap.<String, Object>builder()
                            .put("aggregations", node.getAggregations())
                            .put("groupingSets", node.getGroupingSets())
                            .put("preGroupedSymbols", node.getPreGroupedSymbols())
                            .put("step", node.getStep())
                            .put("hashSymbol", node.getHashSymbol())
                            .put("groupIdSymbol", node.getGroupIdSymbol())
                            .put("outputs", node.getOutputSymbols())
                            .build());
        }

        @Override
        public PlanNodeSignature visitTableScan(TableScanNode node, Void context)
        {
            return signature(
                    node,
                    context,
                    true,
                    ImmutableMap.of(
                            "table.catalogName", node.getTable().getCatalogName(),
                            "table", node.getTable().getConnectorHandle(),
                            "outputSymbols", node.getOutputSymbols(),
                            "assignments", node.getAssignments(),
                            "updateTarget", node.isUpdateTarget()));
        }

        @Override
        public PlanNodeSignature visitProject(ProjectNode node, Void context)
        {
            return signature(
                    node,
                    context,
                    true,
                    ImmutableMap.of("assignments", node.getAssignments()));
        }

        @Override
        public PlanNodeSignature visitFilter(FilterNode node, Void context)
        {
            return signature(
                    node,
                    context,
                    true,
                    ImmutableMap.of("predicate", node.getPredicate()));
        }

        @Override
        public PlanNodeSignature visitJoin(JoinNode node, Void context)
        {
            PlanNodeSignature left = node.getLeft().accept(this, context);
            PlanNodeSignature right = node.getRight().accept(this, context);

            return new GenericPlanNodeSignature(
                    left.canCache(),
                    node,
                    ImmutableList.of(left, right),
                    ImmutableMap.<String, Object>builder()
                            .put("type", node.getType())
                            .put("criteria", node.getCriteria())
                            .put("leftOutputSymbols", node.getLeftOutputSymbols())
                            .put("rightOutputSymbols", node.getRightOutputSymbols())
                            .put("maySkipOutputDuplicates", node.isMaySkipOutputDuplicates())
                            .put("filter", node.getFilter())
                            .put("leftHashSymbol", node.getLeftHashSymbol())
                            .put("rightHashSymbol", node.getRightHashSymbol())
                            .put("distributionType", node.getDistributionType())
                            .put("spillable", node.isSpillable())
                            .put("dynamicFilters", node.getDynamicFilters())
                            .build());
        }

        @Override
        public PlanNodeSignature visitRemoteSource(RemoteSourceNode node, Void context)
        {
            return signature(
                    node,
                    context,
                    false,
                    ImmutableMap.of(
                            "sourceFragmentIds", node.getSourceFragmentIds(),
                            "outputs", node.getOutputSymbols(),
                            "orderingScheme", node.getOrderingScheme(),
                            "exchangeType", node.getExchangeType()));
        }

        @Override
        public PlanNodeSignature visitTopN(TopNNode node, Void context)
        {
            return signature(
                    node,
                    context,
                    true,
                    ImmutableMap.of(
                            "count", node.getCount(),
                            "orderingScheme", node.getOrderingScheme(),
                            "step", node.getStep()));
        }

        @Override
        public PlanNodeSignature visitOutput(OutputNode node, Void context)
        {
            return signature(
                    node,
                    context,
                    true,
                    ImmutableMap.of(
                            "columnNames", node.getColumnNames(),
                            "outputs", node.getOutputSymbols()));
        }

        @Override
        public PlanNodeSignature visitOffset(OffsetNode node, Void context)
        {
            return signature(
                    node,
                    context,
                    true,
                    ImmutableMap.of(
                            "count", node.getCount()));
        }

        @Override
        public PlanNodeSignature visitLimit(LimitNode node, Void context)
        {
            return signature(
                    node,
                    context,
                    true,
                    ImmutableMap.of(
                            "count", node.getCount(),
                            "tiesResolvingScheme", node.getTiesResolvingScheme(),
                            "partial", node.isPartial(),
                            "preSortedInputs", node.getPreSortedInputs()));
        }

        @Override
        public PlanNodeSignature visitDistinctLimit(DistinctLimitNode node, Void context)
        {
            return signature(
                    node,
                    context,
                    true,
                    ImmutableMap.of(
                            "limit", node.getLimit(),
                            "partial", node.isPartial(),
                            "distinctSymbols", node.getDistinctSymbols(),
                            "hashSymbol", node.getHashSymbol()));
        }

        @Override
        public PlanNodeSignature visitSample(SampleNode node, Void context)
        {
            return signature(
                    node,
                    context,
                    false, // TODO lysy: maybe true?
                    ImmutableMap.of(
                            "sampleRatio", node.getSampleRatio(),
                            "sampleType", node.getSampleType()));
        }

        @Override
        public PlanNodeSignature visitExplainAnalyze(ExplainAnalyzeNode node, Void context)
        {
            return signature(
                    node,
                    context,
                    true,
                    ImmutableMap.of(
                            "outputSymbol", node.getOutputSymbol(),
                            "actualOutputs", node.getActualOutputs(),
                            "verbose", node.isVerbose()));
        }

        @Override
        public PlanNodeSignature visitValues(ValuesNode node, Void context)
        {
            return signature(
                    node,
                    context,
                    true,
                    ImmutableMap.of(
                            "outputSymbols", node.getOutputSymbols(),
                            "rowCount", node.getRowCount(),
                            "rows", node.getRows()));
        }

        @Override
        public PlanNodeSignature visitIndexSource(IndexSourceNode node, Void context)
        {
            return signature(
                    node,
                    context,
                    true,
                    ImmutableMap.<String, Object>builder()
                            .put("indexHandle.catalogName", node.getIndexHandle().getCatalogName())
                            .put("indexHandle", node.getIndexHandle().getConnectorHandle())
                            .put("tableHandle.catalogName", node.getTableHandle().getCatalogName())
                            .put("tableHandle", node.getTableHandle().getConnectorHandle())
                            .put("lookupSymbols", node.getLookupSymbols())
                            .put("outputSymbols", node.getOutputSymbols())
                            .put("assignments", node.getAssignments())
                            .build());
        }

        @Override
        public PlanNodeSignature visitSemiJoin(SemiJoinNode node, Void context)
        {
            return signature(
                    node,
                    context,
                    false, // TODO lysy: this could be true
                    ImmutableMap.<String, Object>builder()
                            .put("sourceJoinSymbol", node.getSourceJoinSymbol())
                            .put("filteringSourceJoinSymbol", node.getFilteringSource())
                            .put("semiJoinOutput", node.getSemiJoinOutput())
                            .put("sourceHashSymbol", node.getSourceHashSymbol())
                            .put("filteringSourceHashSymbol", node.getFilteringSourceJoinSymbol())
                            .put("distributionType", node.getDistributionType())
                            .put("dynamicFilterId", node.getDynamicFilterId())
                            .build());
        }

        @Override
        public PlanNodeSignature visitSpatialJoin(SpatialJoinNode node, Void context)
        {
            return signature(
                    node,
                    context,
                    false, // TODO lysy: this could be true
                    ImmutableMap.<String, Object>builder()
                            .put("type", node.getType())
                            .put("outputSymbols", node.getOutputSymbols())
                            .put("filter", node.getFilter())
                            .put("leftPartitionSymbol", node.getLeftPartitionSymbol())
                            .put("rightPartitionSymbol", node.getRightPartitionSymbol())
                            .put("kdbTree", node.getKdbTree())
                            .put("distributionType", node.getDistributionType())
                            .build());
        }

        @Override
        public PlanNodeSignature visitIndexJoin(IndexJoinNode node, Void context)
        {
            return signature(
                    node,
                    context,
                    false, // TODO lysy: this could be true
                    ImmutableMap.<String, Object>builder()
                            .put("type", node.getType())
                            .put("criteria", node.getCriteria())
                            .put("probeHashSymbol", node.getProbeHashSymbol())
                            .put("indexHashSymbol", node.getIndexHashSymbol())
                            .build());
        }

        @Override
        public PlanNodeSignature visitSort(SortNode node, Void context)
        {
            return signature(
                    node,
                    context,
                    true,
                    ImmutableMap.of(
                            "orderingScheme", node.getOrderingScheme(),
                            "partial", node.isPartial()));
        }

        @Override
        public PlanNodeSignature visitWindow(WindowNode node, Void context)
        {
            return signature(
                    node,
                    context,
                    true,
                    ImmutableMap.of(
                            "prePartitionedInputs", node.getPrePartitionedInputs(),
                            "specification", node.getSpecification(),
                            "preSortedOrderPrefix", node.getPreSortedOrderPrefix(),
                            "windowFunctions", node.getWindowFunctions(),
                            "hashSymbol", node.getHashSymbol()));
        }

        @Override
        public PlanNodeSignature visitRefreshMaterializedView(RefreshMaterializedViewNode node, Void context)
        {
            return noCache(node, context);
        }

        @Override
        public PlanNodeSignature visitTableWriter(TableWriterNode node, Void context)
        {
            return noCache(node, context);
        }

        @Override
        public PlanNodeSignature visitDelete(DeleteNode node, Void context)
        {
            return noCache(node, context);
        }

        @Override
        public PlanNodeSignature visitUpdate(UpdateNode node, Void context)
        {
            return noCache(node, context);
        }

        @Override
        public PlanNodeSignature visitTableExecute(TableExecuteNode node, Void context)
        {
            return noCache(node, context);
        }

        @Override
        public PlanNodeSignature visitTableDelete(TableDeleteNode node, Void context)
        {
            return noCache(node, context);
        }

        @Override
        public PlanNodeSignature visitTableFinish(TableFinishNode node, Void context)
        {
            return noCache(node, context);
        }

        @Override
        public PlanNodeSignature visitStatisticsWriterNode(StatisticsWriterNode node, Void context)
        {
            return noCache(node, context);
        }

        @Override
        public PlanNodeSignature visitUnion(UnionNode node, Void context)
        {
            return signature(
                    node,
                    context,
                    true,
                    ImmutableMap.of(
                            "sources", node.getSources(),
                            "outputToInputs", node.getSymbolMapping(),
                            "outputs", node.getOutputSymbols()));
        }

        @Override
        public PlanNodeSignature visitIntersect(IntersectNode node, Void context)
        {
            return signature(
                    node,
                    context,
                    true,
                    ImmutableMap.of(
                            "distinct", node.isDistinct(),
                            "sources", node.getSources(),
                            "outputToInputs", node.getSymbolMapping(),
                            "outputs", node.getOutputSymbols()));
        }

        @Override
        public PlanNodeSignature visitExcept(ExceptNode node, Void context)
        {
            return signature(
                    node,
                    context,
                    true,
                    ImmutableMap.of(
                            "distinct", node.isDistinct(),
                            "sources", node.getSources(),
                            "outputToInputs", node.getSymbolMapping(),
                            "outputs", node.getOutputSymbols()));
        }

        @Override
        public PlanNodeSignature visitUnnest(UnnestNode node, Void context)
        {
            return signature(
                    node,
                    context,
                    true,
                    ImmutableMap.of(
                            "replicateSymbols", node.getReplicateSymbols(),
                            "mappings", node.getMappings(),
                            "ordinalitySymbol", node.getOrdinalitySymbol(),
                            "joinType", node.getJoinType(),
                            "filter", node.getFilter()));
        }

        @Override
        public PlanNodeSignature visitMarkDistinct(MarkDistinctNode node, Void context)
        {
            return signature(
                    node,
                    context,
                    true,
                    ImmutableMap.of(
                            "markerSymbol", node.getMarkerSymbol(),
                            "hashSymbol", node.getHashSymbol(),
                            "distinctSymbols", node.getDistinctSymbols()));
        }

        @Override
        public PlanNodeSignature visitGroupId(GroupIdNode node, Void context)
        {
            return signature(
                    node,
                    context,
                    true,
                    ImmutableMap.of(
                            "groupingSets", node.getGroupingSets(),
                            "groupingColumns", node.getGroupingColumns(),
                            "aggregationArguments", node.getAggregationArguments(),
                            "groupIdSymbol", node.getGroupIdSymbol()));
        }

        @Override
        public PlanNodeSignature visitRowNumber(RowNumberNode node, Void context)
        {
            return signature(
                    node,
                    context,
                    true,
                    ImmutableMap.of(
                            "partitionBy", node.getPartitionBy(),
                            "orderSensitive", node.isOrderSensitive(),
                            "maxRowCountPerPartition", node.getMaxRowCountPerPartition(),
                            "rowNumberSymbol", node.getRowNumberSymbol(),
                            "hashSymbol", node.getHashSymbol()));
        }

        @Override
        public PlanNodeSignature visitTopNRanking(TopNRankingNode node, Void context)
        {
            return signature(
                    node,
                    context,
                    true,
                    ImmutableMap.<String, Object>builder()
                            .put("specification", node.getSpecification())
                            .put("rankingType", node.getRankingType())
                            .put("rankingSymbol", node.getRankingSymbol())
                            .put("maxRankingPerPartition", node.getMaxRankingPerPartition())
                            .put("partial", node.isPartial())
                            .put("hashSymbol", node.getHashSymbol())
                            .build());
        }

        @Override
        public PlanNodeSignature visitExchange(ExchangeNode node, Void context)
        {
            return signature(
                    node,
                    context,
                    false,
                    ImmutableMap.of(
                            "type", node.getType(),
                            "scope", node.getScope(),
                            "partitioningScheme", node.getPartitioningScheme(),
                            "inputs", node.getInputs(),
                            "orderingScheme", node.getOrderingScheme()));
        }

        @Override
        public PlanNodeSignature visitEnforceSingleRow(EnforceSingleRowNode node, Void context)
        {
            return signature(
                    node,
                    context,
                    true,
                    ImmutableMap.of());
        }

        @Override
        public PlanNodeSignature visitApply(ApplyNode node, Void context)
        {
            return signature(
                    node,
                    context,
                    false,
                    ImmutableMap.of(
                            "correlation", node.getCorrelation(),
                            "subqueryAssignments", node.getSubqueryAssignments(),
                            "originSubquery", node.getOriginSubquery()));
        }

        @Override
        public PlanNodeSignature visitAssignUniqueId(AssignUniqueId node, Void context)
        {
            return signature(
                    node,
                    context,
                    true,
                    ImmutableMap.of("idColumn", node.getIdColumn()));
        }

        @Override
        public PlanNodeSignature visitGroupReference(GroupReference node, Void context)
        {
            return new GenericPlanNodeSignature(
                    false,
                    node,
                    ImmutableList.of(),
                    ImmutableMap.of(
                            "groupId", node.getGroupId(),
                            "outputs", node.getOutputSymbols()));
        }

        @Override
        public PlanNodeSignature visitCorrelatedJoin(CorrelatedJoinNode node, Void context)
        {
            return signature(
                    node,
                    context,
                    false,
                    ImmutableMap.of(
                            "correlation", node.getCorrelation(),
                            "type", node.getType(),
                            "filter", node.getFilter(),
                            "originSubquery", node.getOriginSubquery()));
        }

        @Override
        public PlanNodeSignature visitPatternRecognition(PatternRecognitionNode node, Void context)
        {
            return signature(
                    node,
                    context,
                    true,
                    ImmutableMap.<String, Object>builder()
                            .put("specification", node.getSpecification())
                            .put("hashSymbol", node.getHashSymbol())
                            .put("prePartitionedInputs", node.getPrePartitionedInputs())
                            .put("preSortedOrderPrefix", node.getPreSortedOrderPrefix())
                            .put("windowFunctions", node.getWindowFunctions())
                            .put("measures", node.getMeasures())
                            .put("commonBaseFrame", node.getCommonBaseFrame())
                            .put("rowsPerMatch", node.getRowsPerMatch())
                            .put("skipToLabel", node.getSkipToLabel())
                            .put("skipToPosition", node.getSkipToPosition())
                            .put("pattern", node.getPattern())
                            .put("subsets", node.getSubsets())
                            .put("variableDefinitions", node.getVariableDefinitions())
                            .build());
        }

        private PlanNodeSignature noCache(PlanNode node, Void context)
        {
            return signature(
                    node,
                    context,
                    false,
                    ImmutableMap.of("node", node));
        }

        private PlanNodeSignature signature(PlanNode node, Void context, boolean canCache, ImmutableMap<String, Object> nodeProperties)
        {
            ImmutableList<PlanNodeSignature> sources = node.getSources().stream().map(source -> source.accept(this, context)).collect(toImmutableList());
            return new GenericPlanNodeSignature(
                    canCache && sources.stream().allMatch(PlanNodeSignature::canCache),
                    node,
                    sources,
                    nodeProperties);
        }
    }

    class GenericPlanNodeSignature
            implements PlanNodeSignature
    {
        private final boolean canCache;
        private final PlanNode node;
        private final List<PlanNodeSignature> sources;
        private final Map<String, Object> nodeProperties;

        public GenericPlanNodeSignature(boolean canCache, PlanNode node, List<PlanNodeSignature> sources)
        {
            this(canCache, node, sources, ImmutableMap.of());
        }

        public GenericPlanNodeSignature(boolean canCache, PlanNode node, List<PlanNodeSignature> sources, Map<String, Object> nodeProperties)
        {
            this.canCache = canCache;
            this.node = requireNonNull(node, "node is null");
            this.sources = requireNonNull(sources, "sources is null");
            this.nodeProperties = nodeProperties;
        }

        public boolean canCache()
        {
            return canCache;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            GenericPlanNodeSignature that = (GenericPlanNodeSignature) o;
            return node.getClass().equals(that.node.getClass())
                    && nodeProperties.equals(that.nodeProperties)
                    && sources.equals(that.sources);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(node.getClass(), nodeProperties, sources);
        }

        @Override
        public String toString()
        {
            return MoreObjects.toStringHelper(this)
                    .add("node", node.getClass())
                    .add("canCache", canCache)
                    .add("nodeProperties", nodeProperties)
                    .add("sources", sources)
                    .toString();
        }
    }
}
