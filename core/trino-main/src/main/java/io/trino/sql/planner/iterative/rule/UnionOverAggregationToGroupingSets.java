package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.spi.connector.ColumnHandle;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.GroupIdNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.UnionNode;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.plan.Patterns.union;

public class UnionOverAggregationToGroupingSets
        implements Rule<UnionNode>
{
    private static final Pattern<UnionNode> PATTERN = union();

    @Override
    public Pattern<UnionNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(UnionNode node, Captures captures, Context context)
    {
        ImmutableList.Builder<AggregationNode> sourcesBuilder = ImmutableList.builder();
        for (PlanNode source : node.getSources()) {
            PlanNode resolved = context.getLookup().resolve(source);
            if (resolved instanceof AggregationNode aggregationNode) {
                sourcesBuilder.add(aggregationNode);
                continue;
            }

            while (resolved instanceof ProjectNode projectNode) {
                resolved = context.getLookup().resolve(projectNode.getSource());
            }
            if (resolved instanceof AggregationNode aggregationNode) {
                sourcesBuilder.add(aggregationNode);
                continue;
            }
            // unsupported union source type
            return Result.empty();
        }

        List<AggregationNode> sources = sourcesBuilder.build();
        checkArgument(!sources.isEmpty(), "sources is empty");
        AggregationNode firstAggregation = sources.get(0);
        PlanNode firstAggregationSource = context.getLookup().resolve(firstAggregation.getSource());
        if (!(firstAggregationSource instanceof TableScanNode firstTableScan)) {
            return Result.empty();
        }

        Map<ColumnHandle, Symbol> targetColumnSymbol = new HashMap<>();
        for (int i = 1; i < sources.size(); i++) {
            AggregationNode aggregationNode = sources.get(i);
            PlanNode source = context.getLookup().resolve(aggregationNode.getSource());
            if (!(source instanceof TableScanNode tableScan)) {
                return Result.empty();
            }
            if (!tableScanEquals(firstTableScan, tableScan)) {
                return Result.empty();
            }
            if (!aggregationsEquals(firstAggregation.getAggregations(), firstTableScan, aggregationNode.getAggregations(), tableScan)) {
                return Result.empty();
            }
        }

        Symbol groupIdSymbol = context.getSymbolAllocator().newSymbol("groupId", BIGINT);
        List<List<Symbol>> groupingSets = sources.stream()
                .map(aggregationNode -> aggregationNode.getGroupingKeys().stream().map(targetColumnSymbol::get).collect(toImmutableList()))
                .collect(toImmutableList());
        Map<Symbol, Symbol> groupingColumns = groupingSets.stream()
                .flatMap(Collection::stream)
                .distinct()
                .collect(toImmutableMap(symbol -> context.getSymbolAllocator().newSymbol(symbol, "gid"), Function.identity()));

        return Result.ofPlanNode(AggregationNode.builderFrom(firstAggregation)
                .setSource(new GroupIdNode(
                        context.getIdAllocator().getNextId(),
                        firstTableScan,
                        groupingSets,
                        groupingColumns,
                        firstTableScan.getOutputSymbols(),
                        groupIdSymbol))
                .build());
    }

    private boolean aggregationsEquals(
            Map<Symbol, AggregationNode.Aggregation> leftAggregations,
            TableScanNode leftTableScan,
            Map<Symbol, AggregationNode.Aggregation> rightAggregations,
            TableScanNode rightTableScan)
    {
        if (!(leftAggregations.size() == rightAggregations.size())) {
            return false;
        }

//        for (Map.Entry<Symbol, AggregationNode.Aggregation> leftEntry : leftAggregations.entrySet()) {
//            leftTableScan.getAssignments().get(leftEntry.getKey())
//        }
        return true;
    }

    private boolean tableScanEquals(TableScanNode left, TableScanNode right)
    {
        return left.getTable().equals(right.getTable())
                && left.getAssignments().keySet().equals(right.getAssignments().keySet())
                && left.getUseConnectorNodePartitioning().equals(right.getUseConnectorNodePartitioning())
                && left.isUpdateTarget() == right.isUpdateTarget()
                && left.getEnforcedConstraint().equals(right.getEnforcedConstraint());
    }
}
