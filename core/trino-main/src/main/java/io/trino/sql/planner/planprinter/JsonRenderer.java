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

import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.json.JsonCodec;
import io.trino.cost.PlanCostEstimate;
import io.trino.cost.PlanNodeStatsAndCostSummary;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.sql.planner.Symbol;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.planner.planprinter.DistributedPlanRepresentation.DistributedPlanStats;
import static io.trino.sql.planner.planprinter.DistributedPlanRepresentation.EstimateStats;
import static io.trino.sql.planner.planprinter.DistributedPlanRepresentation.JsonWindowOperatorStats;
import static io.trino.sql.planner.planprinter.NodeRepresentation.TypedSymbol;
import static io.trino.sql.planner.planprinter.util.RendererUtils.formatAsCpuCost;
import static io.trino.sql.planner.planprinter.util.RendererUtils.formatAsDataSize;
import static io.trino.sql.planner.planprinter.util.RendererUtils.formatAsLong;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class JsonRenderer
        implements Renderer<String>
{
    private static final JsonCodec<JsonRenderedNode> CODEC = JsonCodec.jsonCodec(JsonRenderedNode.class);
    private static final JsonCodec<JsonDistributedPlanFragments> DISTRIBUTED_PLAN_CODEC
            = JsonCodec.jsonCodec(JsonDistributedPlanFragments.class);

    @Override
    public String render(PlanRepresentation plan)
    {
        return CODEC.toJson(renderJson(plan, plan.getRoot()));
    }

    public String render(JsonDistributedPlanFragments planFragments)
    {
        return DISTRIBUTED_PLAN_CODEC.toJson(planFragments);
    }

    public Map<Integer, List<DistributedPlanRepresentation>> render(PlanRepresentation plan, boolean verbose, Integer level)
    {
        NodeRepresentation node = plan.getRoot();
        Map<Integer, List<DistributedPlanRepresentation>> distributedPlanRepresentationMap = new HashMap<>();
        distributedPlanRepresentationMap.computeIfAbsent(level, i -> new ArrayList<>()).add(getPlanRepresentation(plan, node, verbose, level));
        return distributedPlanRepresentationMap;
    }

    private DistributedPlanRepresentation getPlanRepresentation(PlanRepresentation plan, NodeRepresentation node, Boolean verbose, Integer level)
    {
        Optional<PlanNodeStatsAndCostSummary> planNodeStatsAndCostSummary = getReorderJoinStatsAndCost(node, verbose);
        List<EstimateStats> estimateStats = getEstimates(plan, node);
        DistributedPlanStats distributedPlanStats = getStats(plan, node);
        List<NodeRepresentation> children = node.getChildren().stream()
                .map(plan::getNode)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toList());
        List<DistributedPlanRepresentation> childrenRepresentation = new ArrayList<>();
        for (NodeRepresentation child : children) {
            childrenRepresentation.add(getPlanRepresentation(plan, child, verbose, level + 1));
        }

        Optional<PlanNodeStats> nodeStats = node.getStats();
        Optional<JsonWindowOperatorStats> jsonWindowOperatorStats = Optional.empty();
        if (nodeStats.isPresent() && (nodeStats.get() instanceof WindowPlanNodeStats) && verbose) {
            WindowOperatorStats windowOperatorStats = ((WindowPlanNodeStats) nodeStats.get()).getWindowOperatorStats();
            jsonWindowOperatorStats = Optional.of(new JsonWindowOperatorStats(
                    windowOperatorStats.getActiveDrivers(),
                    windowOperatorStats.getTotalDrivers(),
                    windowOperatorStats.getIndexSizeStdDev(),
                    windowOperatorStats.getIndexPositionsStdDev(),
                    windowOperatorStats.getIndexCountPerDriverStdDev(),
                    windowOperatorStats.getRowsPerDriverStdDev(),
                    windowOperatorStats.getPartitionRowsStdDev()));
        }

        return new DistributedPlanRepresentation(
                node.getName(),
                node.getDescriptor(),
                node.getOutputs(),
                node.getDetails(),
                planNodeStatsAndCostSummary,
                estimateStats,
                distributedPlanStats,
                childrenRepresentation,
                jsonWindowOperatorStats);
    }

    private List<EstimateStats> getEstimates(PlanRepresentation plan, NodeRepresentation node)
    {
        if (node.getEstimatedStats().stream().allMatch(PlanNodeStatsEstimate::isOutputRowCountUnknown) &&
                node.getEstimatedCost().stream().allMatch(c -> c.equals(PlanCostEstimate.unknown()))) {
            return new ArrayList<>();
        }

        List<EstimateStats> estimateStats = new ArrayList<>();
        int estimateCount = node.getEstimatedStats().size();

        for (int i = 0; i < estimateCount; i++) {
            PlanNodeStatsEstimate stats = node.getEstimatedStats().get(i);
            PlanCostEstimate cost = node.getEstimatedCost().get(i);

            List<Symbol> outputSymbols = node.getOutputs().stream()
                    .map(NodeRepresentation.TypedSymbol::getSymbol)
                    .collect(toList());

            estimateStats.add(new EstimateStats(
                    formatAsLong(stats.getOutputRowCount()),
                    formatAsDataSize(stats.getOutputSizeInBytes(outputSymbols, plan.getTypes())),
                    formatAsCpuCost(cost.getCpuCost()),
                    formatAsDataSize(cost.getMaxMemory()),
                    formatAsDataSize(cost.getNetworkCost())));
        }

        return estimateStats;
    }

    private Optional<PlanNodeStatsAndCostSummary> getReorderJoinStatsAndCost(NodeRepresentation node, boolean verbose)
    {
        if (verbose && node.getReorderJoinStatsAndCost().isPresent()) {
            return node.getReorderJoinStatsAndCost();
        }
        return Optional.empty();
    }

    private DistributedPlanStats getStats(PlanRepresentation plan, NodeRepresentation node)
    {
        if (node.getStats().isEmpty() || !(plan.getTotalCpuTime().isPresent() && plan.getTotalScheduledTime().isPresent())) {
            return null;
        }

        return DistributedPlanStats.of(node.getStats().get(), plan);
    }

    JsonRenderedNode renderJson(PlanRepresentation plan, NodeRepresentation node)
    {
        List<JsonRenderedNode> children = node.getChildren().stream()
                .map(plan::getNode)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(n -> renderJson(plan, n))
                .collect(toImmutableList());

        return new JsonRenderedNode(
                node.getId().toString(),
                node.getName(),
                node.getDescriptor(),
                node.getOutputs(),
                node.getDetails(),
                node.getEstimates(plan.getTypes()),
                children);
    }

    public static class JsonRenderedNode
    {
        private final String id;
        private final String name;
        private final Map<String, String> descriptor;
        private final List<TypedSymbol> outputs;
        private final List<String> details;
        private final List<PlanNodeStatsAndCostSummary> estimates;
        private final List<JsonRenderedNode> children;

        public JsonRenderedNode(
                String id,
                String name,
                Map<String, String> descriptor,
                List<TypedSymbol> outputs,
                List<String> details,
                List<PlanNodeStatsAndCostSummary> estimates,
                List<JsonRenderedNode> children)
        {
            this.id = requireNonNull(id, "id is null");
            this.name = requireNonNull(name, "name is null");
            this.descriptor = requireNonNull(descriptor, "descriptor is null");
            this.outputs = requireNonNull(outputs, "outputs is null");
            this.details = requireNonNull(details, "details is null");
            this.estimates = requireNonNull(estimates, "estimates is null");
            this.children = requireNonNull(children, "children is null");
        }

        @JsonProperty
        public String getId()
        {
            return id;
        }

        @JsonProperty
        public String getName()
        {
            return name;
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
        public List<String> getDetails()
        {
            return details;
        }

        @JsonProperty
        public List<PlanNodeStatsAndCostSummary> getEstimates()
        {
            return estimates;
        }

        @JsonProperty
        public List<JsonRenderedNode> getChildren()
        {
            return children;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (!(o instanceof JsonRenderedNode)) {
                return false;
            }
            JsonRenderedNode that = (JsonRenderedNode) o;
            return id.equals(that.id)
                    && name.equals(that.name)
                    && descriptor.equals(that.descriptor)
                    && outputs.equals(that.outputs)
                    && details.equals(that.details)
                    && estimates.equals(that.estimates)
                    && children.equals(that.children);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(id, name, descriptor, outputs, details, estimates, children);
        }
    }
}
