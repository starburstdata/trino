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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import io.trino.Session;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.LambdaExpression;

import java.util.AbstractMap.SimpleEntry;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.intersection;
import static io.trino.SystemSessionProperties.isPushPartialAggregationThroughJoin;
import static io.trino.sql.planner.plan.AggregationNode.Step.INTERMEDIATE;
import static io.trino.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static io.trino.sql.planner.plan.AggregationNode.singleGroupingSet;
import static io.trino.sql.planner.plan.Patterns.aggregation;
import static io.trino.sql.planner.plan.Patterns.join;
import static io.trino.sql.planner.plan.Patterns.source;

public class PushPartialAggregationThroughJoin
        implements Rule<AggregationNode>
{
    private static final Capture<JoinNode> JOIN_NODE = Capture.newCapture();

    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .matching(PushPartialAggregationThroughJoin::isSupportedAggregationNode)
            .with(source().matching(join().capturedAs(JOIN_NODE)));

    private static boolean isSupportedAggregationNode(AggregationNode aggregationNode)
    {
        // Don't split streaming aggregations
        if (aggregationNode.isStreamable()) {
            return false;
        }

        if (aggregationNode.getHashSymbol().isPresent()) {
            // TODO: add support for hash symbol in aggregation node
            return false;
        }
        return aggregationNode.getStep() == PARTIAL && aggregationNode.getGroupingSetCount() == 1;
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isPushPartialAggregationThroughJoin(session);
    }

    @Override
    public Result apply(AggregationNode aggregationNode, Captures captures, Context context)
    {
        JoinNode joinNode = captures.get(JOIN_NODE);

        if (joinNode.getType() != JoinNode.Type.INNER) {
            return Result.empty();
        }

        if (allAggregationsOn(aggregationNode.getAggregations(), joinNode.getLeft().getOutputSymbols())) {
            return Result.ofPlanNode(pushPartialToLeftChild(aggregationNode, joinNode, context));
        }
        if (allAggregationsOn(aggregationNode.getAggregations(), joinNode.getRight().getOutputSymbols())) {
            return Result.ofPlanNode(pushPartialToRightChild(aggregationNode, joinNode, context));
        }

        return Result.empty();
    }

    private static boolean allAggregationsOn(Map<Symbol, Aggregation> aggregations, List<Symbol> symbols)
    {
        Set<Symbol> inputs = aggregations.values().stream()
                .map(SymbolsExtractor::extractAll)
                .flatMap(List::stream)
                .collect(toImmutableSet());
        return symbols.containsAll(inputs);
    }

    private PlanNode pushPartialToLeftChild(AggregationNode node, JoinNode child, Context context)
    {
        Set<Symbol> joinLeftChildSymbols = ImmutableSet.copyOf(child.getLeft().getOutputSymbols());
        List<Symbol> groupingSet = getPushedDownGroupingSet(node, joinLeftChildSymbols, intersection(getJoinRequiredSymbols(child), joinLeftChildSymbols));
        Map<Symbol, Symbol> partialToIntermediateAggregationSymbolMapping = getPartialToIntermediateAggregationSymbolMapping(node, context);
        AggregationNode pushedAggregation = replaceAggregationSource(node, child.getLeft(), groupingSet, partialToIntermediateAggregationSymbolMapping, context);
        return pushPartialToJoin(node, child, pushedAggregation, child.getRight(), pushedAggregation, partialToIntermediateAggregationSymbolMapping);
    }

    private PlanNode pushPartialToRightChild(AggregationNode node, JoinNode child, Context context)
    {
        Set<Symbol> joinRightChildSymbols = ImmutableSet.copyOf(child.getRight().getOutputSymbols());
        List<Symbol> groupingSet = getPushedDownGroupingSet(node, joinRightChildSymbols, intersection(getJoinRequiredSymbols(child), joinRightChildSymbols));
        Map<Symbol, Symbol> partialToIntermediateAggregationSymbolMapping = getPartialToIntermediateAggregationSymbolMapping(node, context);
        AggregationNode pushedAggregation = replaceAggregationSource(node, child.getRight(), groupingSet, partialToIntermediateAggregationSymbolMapping, context);
        return pushPartialToJoin(node, child, child.getLeft(), pushedAggregation, pushedAggregation, partialToIntermediateAggregationSymbolMapping);
    }

    private Set<Symbol> getJoinRequiredSymbols(JoinNode node)
    {
        return Streams.concat(
                        node.getCriteria().stream().map(JoinNode.EquiJoinClause::getLeft),
                        node.getCriteria().stream().map(JoinNode.EquiJoinClause::getRight),
                        node.getFilter().map(SymbolsExtractor::extractUnique).orElse(ImmutableSet.of()).stream(),
                        node.getLeftHashSymbol().map(ImmutableSet::of).orElse(ImmutableSet.of()).stream(),
                        node.getRightHashSymbol().map(ImmutableSet::of).orElse(ImmutableSet.of()).stream())
                .collect(toImmutableSet());
    }

    private List<Symbol> getPushedDownGroupingSet(AggregationNode aggregation, Set<Symbol> availableSymbols, Set<Symbol> requiredJoinSymbols)
    {
        List<Symbol> groupingSet = aggregation.getGroupingKeys();

        // keep symbols that are directly from the join's child (availableSymbols)
        List<Symbol> pushedDownGroupingSet = groupingSet.stream()
                .filter(availableSymbols::contains)
                .collect(Collectors.toList());

        // add missing required join symbols to grouping set
        Set<Symbol> existingSymbols = new HashSet<>(pushedDownGroupingSet);
        requiredJoinSymbols.stream()
                .filter(existingSymbols::add)
                .forEach(pushedDownGroupingSet::add);

        return pushedDownGroupingSet;
    }

    private Map<Symbol, Symbol> getPartialToIntermediateAggregationSymbolMapping(AggregationNode aggregation, Context context)
    {
        // use new output symbols for partial aggregations pushed though join
        return aggregation.getAggregations().keySet().stream()
                .map(symbol -> new SimpleEntry<>(context.getSymbolAllocator().newSymbol(symbol), symbol))
                .collect(toImmutableMap(SimpleEntry::getKey, SimpleEntry::getValue));
    }

    private AggregationNode replaceAggregationSource(
            AggregationNode aggregation,
            PlanNode source,
            List<Symbol> groupingKeys,
            Map<Symbol, Symbol> partialToIntermediateAggregationSymbolMapping,
            Context context)
    {
        Map<Symbol, Symbol> intermediateToPartialAggregationSymbolMapping = HashBiMap.create(partialToIntermediateAggregationSymbolMapping).inverse();
        Map<Symbol, AggregationNode.Aggregation> newPartialAggregations = aggregation.getAggregations().entrySet().stream()
                .map(entry -> new SimpleEntry<>(intermediateToPartialAggregationSymbolMapping.get(entry.getKey()), entry.getValue()))
                .collect(toImmutableMap(SimpleEntry::getKey, SimpleEntry::getValue));
        return AggregationNode.builderFrom(aggregation)
                .setSource(source)
                .setId(context.getIdAllocator().getNextId())
                .setGroupingSets(singleGroupingSet(groupingKeys))
                .setPreGroupedSymbols(ImmutableList.of()) // pre-grouped symbols might not exist in join branch
                .setAggregations(newPartialAggregations)
                .build();
    }

    private PlanNode pushPartialToJoin(
            AggregationNode aggregation,
            JoinNode child,
            PlanNode leftChild,
            PlanNode rightChild,
            AggregationNode pushedAggregation,
            Map<Symbol, Symbol> partialToIntermediateAggregationSymbolMapping)
    {
        Map<Symbol, AggregationNode.Aggregation> intermediateAggregations = pushedAggregation.getAggregations().entrySet().stream()
                .map(entry -> new SimpleEntry<>(
                        partialToIntermediateAggregationSymbolMapping.get(entry.getKey()),
                        new AggregationNode.Aggregation(
                                entry.getValue().getResolvedFunction(),
                                ImmutableList.<Expression>builder()
                                        .add(entry.getKey().toSymbolReference())
                                        .addAll(entry.getValue().getArguments().stream()
                                                .filter(LambdaExpression.class::isInstance)
                                                .collect(toImmutableList()))
                                        .build(),
                                false,
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty())))
                .collect(toImmutableMap(SimpleEntry::getKey, SimpleEntry::getValue));
        JoinNode joinNode = new JoinNode(
                child.getId(),
                child.getType(),
                leftChild,
                rightChild,
                child.getCriteria(),
                leftChild.getOutputSymbols(),
                rightChild.getOutputSymbols(),
                child.isMaySkipOutputDuplicates(),
                child.getFilter(),
                child.getLeftHashSymbol(),
                child.getRightHashSymbol(),
                child.getDistributionType(),
                child.isSpillable(),
                child.getDynamicFilters(),
                child.getReorderJoinStatsAndCost());
        return AggregationNode.builderFrom(aggregation)
                .setSource(joinNode)
                .setStep(INTERMEDIATE)
                .setPreGroupedSymbols(ImmutableList.of()) // cannot guarantee that partial aggregation will produce pre-grouped symbols
                .setAggregations(intermediateAggregations)
                .build();
    }
}
