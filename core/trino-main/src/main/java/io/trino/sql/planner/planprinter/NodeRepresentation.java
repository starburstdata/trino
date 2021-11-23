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
package io.trino.sql.planner.planprinter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.cost.PlanCostEstimate;
import io.trino.cost.PlanNodeStatsAndCostSummary;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class NodeRepresentation
{
    private final PlanNodeId id;
    private final String name;
    private final String type;
    private final Map<String, String> descriptor;
    private final List<TypedSymbol> outputs;
    private final List<PlanNodeId> children;
    private final List<PlanFragmentId> remoteSources;
    private final Optional<PlanNodeStats> stats;
    private final List<PlanNodeStatsEstimate> estimatedStats;
    private final List<PlanCostEstimate> estimatedCost;
    private final Optional<PlanNodeStatsAndCostSummary> reorderJoinStatsAndCost;
    private final List<NodeRepresentation> childrenNodeRepresentation;

    private final ImmutableList.Builder<String> details = ImmutableList.builder();

    @JsonCreator
    public NodeRepresentation(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("name") String name,
            @JsonProperty("type") String type,
            @JsonProperty("descriptor") Map<String, String> descriptor,
            @JsonProperty("outputs") List<TypedSymbol> outputs,
            @JsonProperty("stats") Optional<PlanNodeStats> stats,
            @JsonProperty("estimatedStats") List<PlanNodeStatsEstimate> estimatedStats,
            @JsonProperty("estimatedCost") List<PlanCostEstimate> estimatedCost,
            @JsonProperty("reorderJoinStatsAndCost") Optional<PlanNodeStatsAndCostSummary> reorderJoinStatsAndCost,
            @JsonProperty("children") List<PlanNodeId> children,
            @JsonProperty("remoteSources") List<PlanFragmentId> remoteSources,
            @JsonProperty("childrenNodeRepresentation") List<NodeRepresentation> childrenNodeRepresentation)
    {
        this.id = requireNonNull(id, "id is null");
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
        this.descriptor = requireNonNull(descriptor, "descriptor is null");
        this.outputs = requireNonNull(outputs, "outputs is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.estimatedStats = requireNonNull(estimatedStats, "estimatedStats is null");
        this.estimatedCost = requireNonNull(estimatedCost, "estimatedCost is null");
        this.reorderJoinStatsAndCost = requireNonNull(reorderJoinStatsAndCost, "reorderJoinStatsAndCost is null");
        this.children = requireNonNull(children, "children is null");
        this.remoteSources = requireNonNull(remoteSources, "remoteSources is null");
        this.childrenNodeRepresentation = requireNonNull(childrenNodeRepresentation, "childrenNodeRepresentation is null");

        checkArgument(estimatedCost.size() == estimatedStats.size(), "size of cost and stats list does not match");
    }

    public void appendDetails(String string, Object... args)
    {
        if (args.length == 0) {
            details.add(string);
        }
        else {
            details.add(format(string, args));
        }
    }

    @JsonProperty
    public PlanNodeId getId()
    {
        return id;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public String getType()
    {
        return type;
    }

    @JsonProperty
    public Map<String, String> getDescriptor()
    {
        return descriptor;
    }

    @JsonProperty
    public List<TypedSymbol> getOutputs()
    {
        return outputs;
    }

    @JsonProperty
    public List<PlanNodeId> getChildren()
    {
        return children;
    }

    @JsonProperty
    public List<PlanFragmentId> getRemoteSources()
    {
        return remoteSources;
    }

    @JsonProperty
    public List<String> getDetails()
    {
        return details.build();
    }

    @JsonProperty
    public Optional<PlanNodeStats> getStats()
    {
        return stats;
    }

    @JsonProperty
    public List<PlanNodeStatsEstimate> getEstimatedStats()
    {
        return estimatedStats;
    }

    @JsonProperty
    public List<PlanCostEstimate> getEstimatedCost()
    {
        return estimatedCost;
    }

    @JsonProperty
    public Optional<PlanNodeStatsAndCostSummary> getReorderJoinStatsAndCost()
    {
        return reorderJoinStatsAndCost;
    }

    @JsonProperty
    public List<NodeRepresentation> getChildNodeRepresentations()
    {
        return childrenNodeRepresentation;
    }

    public List<PlanNodeStatsAndCostSummary> getEstimates(TypeProvider typeProvider)
    {
        if (getEstimatedStats().stream().allMatch(PlanNodeStatsEstimate::isOutputRowCountUnknown) &&
                getEstimatedCost().stream().allMatch(c -> c.equals(PlanCostEstimate.unknown()))) {
            return ImmutableList.of();
        }

        ImmutableList.Builder<PlanNodeStatsAndCostSummary> estimates = ImmutableList.builder();
        for (int i = 0; i < getEstimatedStats().size(); i++) {
            PlanNodeStatsEstimate stats = getEstimatedStats().get(i);
            PlanCostEstimate cost = getEstimatedCost().get(i);

            List<Symbol> outputSymbols = getOutputs().stream()
                    .map(NodeRepresentation.TypedSymbol::getSymbol)
                    .collect(toImmutableList());

            estimates.add(new PlanNodeStatsAndCostSummary(
                    stats.getOutputRowCount(),
                    stats.getOutputSizeInBytes(outputSymbols, typeProvider),
                    cost.getCpuCost(),
                    cost.getMaxMemory(),
                    cost.getNetworkCost()));
        }

        return estimates.build();
    }

    public static class TypedSymbol
    {
        private final Symbol symbol;
        private final String type;

        @JsonCreator
        public TypedSymbol(@JsonProperty("symbol") Symbol symbol, @JsonProperty("type") String type)
        {
            this.symbol = requireNonNull(symbol, "symbol is null");
            this.type = requireNonNull(type, "type is null");
        }

        @JsonProperty
        public Symbol getSymbol()
        {
            return symbol;
        }

        @JsonProperty
        public String getType()
        {
            return type;
        }

        public static TypedSymbol typedSymbol(String symbol, String type)
        {
            return new TypedSymbol(new Symbol(symbol), type);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (!(o instanceof TypedSymbol)) {
                return false;
            }
            TypedSymbol that = (TypedSymbol) o;
            return symbol.equals(that.symbol)
                    && type.equals(that.type);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(symbol, type);
        }
    }
}
