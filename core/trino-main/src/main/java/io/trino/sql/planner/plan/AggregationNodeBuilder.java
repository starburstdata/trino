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
package io.trino.sql.planner.plan;

import io.trino.sql.planner.Symbol;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class AggregationNodeBuilder
{
    private final AggregationNode node;
    @Nullable
    private PlanNodeId id;
    @Nullable
    private PlanNode source;
    @Nullable
    private Map<Symbol, AggregationNode.Aggregation> aggregations;
    @Nullable
    private AggregationNode.GroupingSetDescriptor groupingSets;
    @Nullable
    private List<Symbol> preGroupedSymbols;
    @Nullable
    private AggregationNode.Step step;
    @Nullable
    private Optional<Symbol> hashSymbol;
    @Nullable
    private Optional<Symbol> groupIdSymbol;
    @Nullable
    Optional<Symbol> rawInputMaskSymbol;

    public AggregationNodeBuilder(AggregationNode node)
    {
        this.node = requireNonNull(node, "node is null");
    }

    public AggregationNodeBuilder setId(PlanNodeId id)
    {
        this.id = id;
        return this;
    }

    public AggregationNodeBuilder setSource(PlanNode source)
    {
        this.source = source;
        return this;
    }

    public AggregationNodeBuilder setAggregations(Map<Symbol, AggregationNode.Aggregation> aggregations)
    {
        this.aggregations = aggregations;
        return this;
    }

    public AggregationNodeBuilder setGroupingSets(AggregationNode.GroupingSetDescriptor groupingSets)
    {
        this.groupingSets = groupingSets;
        return this;
    }

    public AggregationNodeBuilder setPreGroupedSymbols(List<Symbol> preGroupedSymbols)
    {
        this.preGroupedSymbols = preGroupedSymbols;
        return this;
    }

    public AggregationNodeBuilder setStep(AggregationNode.Step step)
    {
        this.step = step;
        return this;
    }

    public AggregationNodeBuilder setHashSymbol(Optional<Symbol> hashSymbol)
    {
        this.hashSymbol = hashSymbol;
        return this;
    }

    public AggregationNodeBuilder setGroupIdSymbol(Optional<Symbol> groupIdSymbol)
    {
        this.groupIdSymbol = groupIdSymbol;
        return this;
    }

    public AggregationNode build()
    {
        return new AggregationNode(
                id != null ? id : node.getId(),
                source != null ? source : node.getSource(),
                aggregations != null ? aggregations : node.getAggregations(),
                groupingSets != null ? groupingSets : node.getGroupingSets(),
                preGroupedSymbols != null ? preGroupedSymbols : node.getPreGroupedSymbols(),
                step != null ? step : node.getStep(),
                hashSymbol != null ? hashSymbol : node.getHashSymbol(),
                groupIdSymbol != null ? groupIdSymbol : node.getGroupIdSymbol(),
                rawInputMaskSymbol != null ? rawInputMaskSymbol : node.getRawInputMaskSymbol());
    }
}
