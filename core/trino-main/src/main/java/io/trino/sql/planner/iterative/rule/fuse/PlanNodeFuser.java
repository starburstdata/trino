package io.trino.sql.planner.iterative.rule.fuse;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.ColumnHandle;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.optimizations.SymbolMapper;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.tree.Expression;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.planner.SymbolsExtractor.extractUnique;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.trino.sql.tree.LogicalExpression.and;
import static io.trino.sql.tree.LogicalExpression.or;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

public class PlanNodeFuser
{
    private final Rule.Context context;

    public PlanNodeFuser(Rule.Context context)
    {
        this.context = requireNonNull(context, "context is null");
    }

    public Optional<FusedPlanNode> fuse(PlanNode left, PlanNode right)
    {
        left = context.getLookup().resolve(left);
        right = context.getLookup().resolve(right);
        if (left instanceof ProjectNode || right instanceof ProjectNode) {
            return fuseProject(left, right);
        }
        if (left instanceof FilterNode || right instanceof FilterNode) {
            return fuseFilter(left, right);
        }

        if (!left.getClass().equals(right.getClass())) {
            return Optional.empty();
        }
        if (left instanceof TableScanNode) {
            return fuseTableScan((TableScanNode) left, (TableScanNode) right);
        }
        if (left instanceof AggregationNode) {
            return fuseAggregation((AggregationNode) left, (AggregationNode) right);
        }
        if (left instanceof JoinNode) {
            return fuseJoin((JoinNode) left, (JoinNode) right);
        }

        return Optional.empty();
    }

    private Optional<FusedPlanNode> fuseFilter(PlanNode left, PlanNode right)
    {
        if (left instanceof FilterNode leftFilter && right instanceof FilterNode rightFilter) {
            return fuseFilter(leftFilter.getSource(), leftFilter.getPredicate(), rightFilter.getSource(), rightFilter.getPredicate());
        }
        if (left instanceof FilterNode leftFilter) {
            return fuseFilter(leftFilter.getSource(), leftFilter.getPredicate(), right, TRUE_LITERAL);
        }
        if (right instanceof FilterNode rightFilter) {
            return fuseFilter(left, TRUE_LITERAL, rightFilter.getSource(), rightFilter.getPredicate());
        }

        throw new IllegalArgumentException(String.format("expected at least one FilterNode but got %s and %s", left, right));
    }

    private Optional<FusedPlanNode> fuseFilter(PlanNode leftSource, Expression leftPredicate, PlanNode rightSource, Expression rightPredicate)
    {
        Optional<FusedPlanNode> maybeFusedSource = fuse(leftSource, rightSource);
        if (maybeFusedSource.isEmpty()) {
            return Optional.empty();
        }
        FusedPlanNode fusedSource = maybeFusedSource.get();

        Expression mappedRightPredicate = fusedSource.symbolMapping().map(rightPredicate);
        if (leftPredicate.equals(mappedRightPredicate)) {
            // simplified version
            return Optional.of(new FusedPlanNode(
                    new FilterNode(
                            context.getIdAllocator().getNextId(),
                            fusedSource.plan(),
                            leftPredicate),
                    fusedSource.symbolMapping(),
                    fusedSource.leftFilter(),
                    fusedSource.rightFilter()));
        }

        return Optional.of(new FusedPlanNode(
                new FilterNode(
                        context.getIdAllocator().getNextId(),
                        fusedSource.plan(),
                        or(leftPredicate, mappedRightPredicate)),
                fusedSource.symbolMapping(),
                and(fusedSource.leftFilter(), leftPredicate),
                and(fusedSource.rightFilter(), mappedRightPredicate)));
    }

    private Optional<FusedPlanNode> fuseProject(PlanNode left, PlanNode right)
    {
        if (left instanceof ProjectNode leftProject && right instanceof ProjectNode rightProject) {
            return fuseProject(leftProject.getSource(), leftProject.getAssignments(), rightProject.getSource(), rightProject.getAssignments());
        }
        if (left instanceof ProjectNode leftProject) {
            return fuseProject(leftProject.getSource(), leftProject.getAssignments(), right, Assignments.of());
        }
        if (right instanceof ProjectNode rightProject) {
            return fuseProject(left, Assignments.of(), rightProject.getSource(), rightProject.getAssignments());
        }

        throw new IllegalArgumentException(String.format("expected at least one ProjectNode but got %s and %s", left, right));
    }

    private Optional<FusedPlanNode> fuseProject(PlanNode leftSource, Assignments leftAssignments, PlanNode rightSource, Assignments rightAssignments)
    {
        Optional<FusedPlanNode> maybeFusedSource = fuse(leftSource, rightSource);
        if (maybeFusedSource.isEmpty()) {
            return Optional.empty();
        }

        FusedPlanNode fusedSource = maybeFusedSource.get();

        Map<Symbol, Expression> assignments = new HashMap<>();
        extractUnique(fusedSource.leftFilter()).forEach(symbol -> assignments.put(symbol, symbol.toSymbolReference()));
        extractUnique(fusedSource.rightFilter()).forEach(symbol -> assignments.put(symbol, symbol.toSymbolReference()));

        FusedSymbolMapping.Builder newMapping = FusedSymbolMapping.builder().withMapping(fusedSource.symbolMapping());
        rightAssignments.forEach(((symbol, expression) -> {
            Expression mappedExpression = fusedSource.symbolMapping().map(expression);
            leftAssignments.entrySet().stream()
                    .filter(entry -> entry.getValue().equals(mappedExpression))
                    .findFirst()
                    .ifPresentOrElse(
                            found -> newMapping.add(symbol, found.getKey()),
                            () -> assignments.put(symbol, mappedExpression));
        }));

        return Optional.of(new FusedPlanNode(
                new ProjectNode(
                        context.getIdAllocator().getNextId(),
                        fusedSource.plan(),
                        Assignments.builder()
                                .putAll(leftAssignments)
                                .putAll((assignments))
                                .build()),
                newMapping.build(),
                fusedSource.leftFilter(),
                fusedSource.rightFilter()));
    }

    private Optional<FusedPlanNode> fuseJoin(JoinNode left, JoinNode right)
    {
        if (!left.getType().equals(INNER) || !left.getType().equals(right.getType())) {
            return Optional.empty();
        }

        Optional<FusedPlanNode> maybeFusedLeft = fuse(left.getLeft(), right.getLeft());
        if (maybeFusedLeft.isEmpty()) {
            return Optional.empty();
        }
        Optional<FusedPlanNode> maybeFusedRight = fuse(left.getRight(), right.getRight());
        if (maybeFusedRight.isEmpty()) {
            return Optional.empty();
        }

        FusedPlanNode fusedLeft = maybeFusedLeft.get();
        FusedPlanNode fusedRight = maybeFusedRight.get();

        FusedSymbolMapping mapping = FusedSymbolMapping.builder()
                .withMapping(fusedLeft.symbolMapping(), fusedRight.symbolMapping())
                .build();

        if (!areEquivalent(left.getCriteria(), right.getCriteria(), mapping)) {
            return Optional.empty();
        }

        Set<Symbol> leftOutputSymbols = ImmutableSet.<Symbol>builder()
                .addAll(left.getLeftOutputSymbols())
                .addAll(extractUnique(fusedLeft.leftFilter()))
                .addAll(extractUnique(fusedLeft.rightFilter()))
                .build();

        Set<Symbol> rightOutputSymbols = ImmutableSet.<Symbol>builder()
                .addAll(left.getRightOutputSymbols())
                .addAll(extractUnique(fusedRight.leftFilter()))
                .addAll(extractUnique(fusedRight.rightFilter()))
                .build();
        return Optional.of(new FusedPlanNode(
                new JoinNode(
                        context.getIdAllocator().getNextId(),
                        left.getType(),
                        fusedLeft.plan(),
                        fusedRight.plan(),
                        left.getCriteria(),
                        ImmutableList.copyOf(leftOutputSymbols),
                        ImmutableList.copyOf(rightOutputSymbols),
                        left.isMaySkipOutputDuplicates(),
                        left.getFilter(),
                        left.getLeftHashSymbol(),
                        left.getRightHashSymbol(),
                        left.getDistributionType(),
                        left.isSpillable(),
                        left.getDynamicFilters(),
                        left.getReorderJoinStatsAndCost()),
                mapping,
                and(fusedLeft.leftFilter(), fusedRight.leftFilter()),
                and(fusedLeft.rightFilter(), fusedRight.rightFilter())));
    }

    private boolean areEquivalent(List<JoinNode.EquiJoinClause> left, List<JoinNode.EquiJoinClause> right, FusedSymbolMapping rightToLeftSymbolMapping)
    {
        // TODO lysy: implement
        return true;
    }

    private Optional<FusedPlanNode> fuseAggregation(AggregationNode left, AggregationNode right)
    {
        if (!left.getGroupingKeys().isEmpty() || !right.getGroupingKeys().isEmpty()) {
            // TODO lysy: drop this assumption
            return Optional.empty();
        }
        if (!left.getStep().equals(SINGLE) || !right.getStep().equals(SINGLE)) { // TODO lysy: precondition?
            return Optional.empty();
        }
        Optional<FusedPlanNode> maybeFusedSource = fuse(left.getSource(), right.getSource());
        if (maybeFusedSource.isEmpty()) {
            return Optional.empty();
        }
        FusedPlanNode fusedSource = maybeFusedSource.get();
        HashMap<Symbol, Aggregation> leftAggregations = new HashMap<>();
        Map<Symbol, Expression> additionalAggregationMasks = new HashMap<>();
        for (Map.Entry<Symbol, Aggregation> aggregationEntry : left.getAggregations().entrySet()) {
            Aggregation aggregation = aggregationEntry.getValue();
            Optional<Symbol> aggregationMask = aggregation.getMask();
            if (!fusedSource.leftFilter().equals(TRUE_LITERAL)) {
                Expression newMask = aggregation.getMask()
                        .map(filter -> (Expression) and(filter.toSymbolReference(), fusedSource.leftFilter()))
                        .orElse(fusedSource.leftFilter());
                Symbol newMaskSymbol = context.getSymbolAllocator().newSymbol("aggr_mask", BOOLEAN);
                additionalAggregationMasks.put(newMaskSymbol, newMask);
                aggregationMask = Optional.of(newMaskSymbol);
            }

            leftAggregations.put(aggregationEntry.getKey(), new Aggregation(
                    aggregation.getResolvedFunction(),
                    aggregation.getArguments(),
                    aggregation.isDistinct(),
                    aggregation.getFilter(),
                    aggregation.getOrderingScheme(),
                    aggregationMask));
        }

        FusedSymbolMapping.Builder newMapping = FusedSymbolMapping.builder().withMapping(fusedSource.symbolMapping());

        HashMap<Symbol, Aggregation> rightAggregations = new HashMap<>();
        for (Map.Entry<Symbol, Aggregation> aggregationEntry : right.getAggregations().entrySet()) {
            Aggregation aggregation = aggregationEntry.getValue();

            Expression newMask = null;
            if (!fusedSource.rightFilter().equals(TRUE_LITERAL)) {
                newMask = aggregation.getMask()
                        .map(mask -> (Expression) and(fusedSource.symbolMapping().map(mask).toSymbolReference(), fusedSource.rightFilter()))
                        .orElse(fusedSource.rightFilter());
            }

            Optional<Symbol> mappedFilter = aggregation.getFilter().map(fusedSource.symbolMapping()::map);
            List<Expression> mappedArguments = aggregation.getArguments().stream().map(fusedSource.symbolMapping()::map).collect(toImmutableList());
            Optional<Symbol> mappedMask = aggregation.getMask().map(fusedSource.symbolMapping()::map);

            // find mappedAggregation in leftAggregations
            boolean found = false;
            for (Map.Entry<Symbol, Aggregation> entry : leftAggregations.entrySet()) {
                Aggregation leftAggregation = entry.getValue();
                if (leftAggregation.getResolvedFunction().equals(aggregation.getResolvedFunction()) &&
                        leftAggregation.getArguments().equals(mappedArguments) &&
                        leftAggregation.isDistinct() == aggregation.isDistinct() &&
                        leftAggregation.getOrderingScheme().equals(aggregation.getOrderingScheme()) &&
                        leftAggregation.getFilter().equals(mappedFilter) &&
                        maskMatches(leftAggregation.getMask(), additionalAggregationMasks, aggregation.getMask(), newMask)) {
                    newMapping.add(aggregationEntry.getKey(), entry.getKey());
                    found = true;
                    break;
                }
            }

            if (!found) {
                Optional<Symbol> aggregationMask = mappedMask;
                if (newMask != null) {
                    Symbol newMaskSymbol = context.getSymbolAllocator().newSymbol("aggr_mask", BOOLEAN);
                    additionalAggregationMasks.put(newMaskSymbol, newMask);
                    aggregationMask = Optional.of(newMaskSymbol);
                }

                Aggregation mappedAggregation = new Aggregation(
                        aggregation.getResolvedFunction(),
                        mappedArguments,
                        aggregation.isDistinct(),
                        mappedFilter,
                        aggregation.getOrderingScheme(),
                        aggregationMask);
                rightAggregations.put(aggregationEntry.getKey(), mappedAggregation);
            }
        }

        PlanNode source = fusedSource.plan();
        if (!additionalAggregationMasks.isEmpty()) {
            source = new ProjectNode(
                    context.getIdAllocator().getNextId(),
                    source,
                    Assignments.builder()
                            .putIdentities(source.getOutputSymbols())
                            .putAll(additionalAggregationMasks)
                            .build());
        }

        return Optional.of(new FusedPlanNode(
                new AggregationNode(
                        context.getIdAllocator().getNextId(),
                        source,
                        ImmutableMap.<Symbol, Aggregation>builder().putAll(leftAggregations).putAll(rightAggregations).buildOrThrow(),
                        left.getGroupingSets(),
                        left.getPreGroupedSymbols(),
                        SINGLE,
                        left.getHashSymbol(),
                        left.getGroupIdSymbol()),
                newMapping.build(),
                TRUE_LITERAL,
                TRUE_LITERAL));
    }

    private boolean maskMatches(Optional<Symbol> optionalMaskSymbol, Map<Symbol, Expression> masks, Optional<Symbol> otherMaskSymbol, @Nullable Expression otherMask)
    {
        if (optionalMaskSymbol.isEmpty()) {
            return otherMaskSymbol.isEmpty() && otherMask == null;
        }
        Symbol maskSymbol = optionalMaskSymbol.get();
        Expression mask = masks.get(maskSymbol);
        if (mask == null) {
            // just not fused mask. symbols must match
            return maskSymbol.equals(otherMaskSymbol.orElse(null)) && otherMask == null;
        }
        return mask.equals(otherMask);
    }

    private Optional<FusedPlanNode> fuseTableScan(TableScanNode left, TableScanNode right)
    {
        if (!left.getTable().equals(right.getTable())) {
            return Optional.empty();
        }
        // TODO lysy: calculate correct outputs and assignments
        Set<Symbol> outputs = new HashSet<>(left.getOutputSymbols());
        Map<Symbol, ColumnHandle> assignments = new HashMap<>(left.getAssignments());
        FusedSymbolMapping.Builder symbolMappingBuilder = FusedSymbolMapping.builder();
        Set<Symbol> rightOutputSymbols = ImmutableSet.copyOf(right.getOutputSymbols());
        right.getAssignments().forEach(((symbol, columnHandle) ->
                left.getAssignments().entrySet().stream().filter(entry -> entry.getValue().equals(columnHandle)).findFirst()
                        .ifPresentOrElse(
                                entry -> {
                                    symbolMappingBuilder.add(symbol, entry.getKey());
                                    if (rightOutputSymbols.contains(symbol)) {
                                        outputs.add(entry.getKey());
                                    }
                                },
                                () -> {
                                    assignments.put(symbol, columnHandle);
                                    if (rightOutputSymbols.contains(symbol)) {
                                        outputs.add(symbol);
                                    }
                                })));
        return Optional.of(new FusedPlanNode(
                new TableScanNode(
                        context.getIdAllocator().getNextId(),
                        left.getTable(),
                        ImmutableList.copyOf(outputs),
                        assignments,
                        left.getEnforcedConstraint(), // TODO lysy: how to fuse 4 following?
                        left.getStatistics(),
                        left.isUpdateTarget(),
                        left.getUseConnectorNodePartitioning()),
                symbolMappingBuilder.build(),
                TRUE_LITERAL,
                TRUE_LITERAL));
    }

    public record FusedPlanNode(PlanNode plan, FusedSymbolMapping symbolMapping, Expression leftFilter, Expression rightFilter)
    {
        public FusedPlanNode(PlanNode plan, FusedSymbolMapping symbolMapping, Expression leftFilter, Expression rightFilter)
        {
            this.plan = requireNonNull(plan, "plan is null");
            this.symbolMapping = requireNonNull(symbolMapping, "symbolMapping is null");
            this.leftFilter = requireNonNull(leftFilter, "leftFilter is null");
            this.rightFilter = requireNonNull(rightFilter, "rightFilter is null");
        }
    }

    public static class FusedSymbolMapping
    {
        private final Map<Symbol, Symbol> mapping;
        private final List<FusedSymbolMapping> delegates;
        private final SymbolMapper symbolMapper;

        public FusedSymbolMapping(Map<Symbol, Symbol> mapping, List<FusedSymbolMapping> delegates)
        {
            this.mapping = requireNonNull(mapping, "mapping is null");
            this.delegates = requireNonNull(delegates, "delegates is null");
            this.symbolMapper = new SymbolMapper(this::map);
        }

        public static Builder builder()
        {
            return new Builder();
        }

        public Symbol map(Symbol symbol)
        {
            for (FusedSymbolMapping delegate : delegates) {
                if (delegate.contains(symbol)) {
                    return delegate.map(symbol);
                }
            }
            Symbol mapped = mapping.get(symbol);
            return mapped != null ? mapped : symbol;
        }

        public boolean contains(Symbol symbol)
        {
            for (FusedSymbolMapping delegate : delegates) {
                if (delegate.contains(symbol)) {
                    return true;
                }
            }
            return mapping.containsKey(symbol);
        }

        public Expression map(Expression argument)
        {
            return symbolMapper.map(argument);
        }

        public static class Builder
        {
            private final ImmutableMap.Builder<Symbol, Symbol> mapping = ImmutableMap.builder();
            private final List<FusedSymbolMapping> delegates = new ArrayList<>();

            public FusedSymbolMapping build()
            {
                return new FusedSymbolMapping(mapping.build(), delegates);
            }

            public Builder withMapping(FusedSymbolMapping... mappings)
            {
                this.delegates.addAll(asList(mappings));

                return this;
            }

            public void add(Symbol from, Symbol to)
            {
                mapping.put(from, to);
            }
        }
    }
}
