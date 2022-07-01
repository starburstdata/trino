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
import io.trino.cost.PlanNodeStatsAndCostSummary;
import io.trino.sql.planner.planprinter.NodeRepresentation.TypedSymbol;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class DistributedPlanRepresentation
{
    private final String name;
    private final Map<String, String> descriptor;
    private final List<TypedSymbol> outputs;
    private final List<String> details;
    private final DistributedPlanStats distributedPlanStats;
    private final Optional<PlanNodeStatsAndCostSummary> planNodeStatsAndCostSummary;
    private final List<EstimateStats> estimateStats;
    private final List<DistributedPlanRepresentation> children;
    private final Optional<JsonWindowOperatorStats> windowOperatorStats;

    @JsonCreator
    public DistributedPlanRepresentation(
            @JsonProperty("name") String name,
            @JsonProperty("descriptor") Map<String, String> descriptor,
            @JsonProperty("layout") List<TypedSymbol> outputs,
            @JsonProperty("details") List<String> details,
            @JsonProperty("reOrderJoinStatsAndCosts") Optional<PlanNodeStatsAndCostSummary> planNodeStatsAndCostSummary,
            @JsonProperty("estimates") List<EstimateStats> estimateStats,
            @JsonProperty("planStats") DistributedPlanStats distributedPlanStats,
            @JsonProperty("children") List<DistributedPlanRepresentation> children,
            @JsonProperty("windowOperatorStats") Optional<JsonWindowOperatorStats> windowOperatorStats)
    {
        this.name = requireNonNull(name, "name is null");
        this.descriptor = requireNonNull(descriptor, "descriptor is null");
        this.outputs = requireNonNull(outputs, "outputs is null");
        this.details = requireNonNull(details, "details is null");
        this.planNodeStatsAndCostSummary = planNodeStatsAndCostSummary;
        this.estimateStats = estimateStats;
        this.distributedPlanStats = distributedPlanStats;
        this.children = children;
        this.windowOperatorStats = windowOperatorStats;
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
    public Optional<PlanNodeStatsAndCostSummary> getPlanNodeStatsAndCostSummary()
    {
        return planNodeStatsAndCostSummary;
    }

    @JsonProperty
    public List<EstimateStats> getEstimateStats()
    {
        return estimateStats;
    }

    @JsonProperty
    public DistributedPlanStats getDistributedPlanStats()
    {
        return distributedPlanStats;
    }

    @JsonProperty
    public List<DistributedPlanRepresentation> getChildren()
    {
        return children;
    }

    @JsonProperty
    public Optional<JsonWindowOperatorStats> getWindowOperatorStats()
    {
        return windowOperatorStats;
    }

    @Immutable
    public static class JsonWindowOperatorStats
    {
        private final int activeDrivers;
        private final int totalDrivers;
        private final double indexSizeStdDev;
        private final double indexPositionsStdDev;
        private final double indexCountPerDriverStdDev;
        private final double rowsPerDriverStdDev;
        private final double partitionRowsStdDev;

        @JsonCreator
        public JsonWindowOperatorStats(
                @JsonProperty("activeDrivers") int activeDrivers,
                @JsonProperty("totalDrivers") int totalDrivers,
                @JsonProperty("indexSizeStdDev") double indexSizeStdDev,
                @JsonProperty("indexPositionsStdDev") double indexPositionsStdDev,
                @JsonProperty("indexCountPerDriverStdDev") double indexCountPerDriverStdDev,
                @JsonProperty("rowsPerDriverStdDev") double rowsPerDriverStdDev,
                @JsonProperty("partitionRowsStdDev") double partitionRowsStdDev)
        {
            this.activeDrivers = activeDrivers;
            this.totalDrivers = totalDrivers;
            this.indexSizeStdDev = indexSizeStdDev;
            this.indexPositionsStdDev = indexPositionsStdDev;
            this.indexCountPerDriverStdDev = indexCountPerDriverStdDev;
            this.rowsPerDriverStdDev = rowsPerDriverStdDev;
            this.partitionRowsStdDev = partitionRowsStdDev;
        }

        @JsonProperty
        public int getActiveDrivers()
        {
            return activeDrivers;
        }

        @JsonProperty
        public int getTotalDrivers()
        {
            return totalDrivers;
        }

        @JsonProperty
        public double getIndexSizeStdDev()
        {
            return indexSizeStdDev;
        }

        @JsonProperty
        public double getIndexPositionsStdDev()
        {
            return indexPositionsStdDev;
        }

        @JsonProperty
        public double getIndexCountPerDriverStdDev()
        {
            return indexCountPerDriverStdDev;
        }

        @JsonProperty
        public double getRowsPerDriverStdDev()
        {
            return rowsPerDriverStdDev;
        }

        @JsonProperty
        public double getPartitionRowsStdDev()
        {
            return partitionRowsStdDev;
        }
    }

    @Immutable
    public static class EstimateStats
    {
        private final String outputRowCount;
        private final String outputSize;
        private final String cpuCost;
        private final String maxMemory;
        private final String networkCost;

        @JsonCreator
        public EstimateStats(
                @JsonProperty("outputRowCount") String outputRowCount,
                @JsonProperty("outputSize") String outputSize,
                @JsonProperty("cpuCost") String cpuCost,
                @JsonProperty("maxMemory") String maxMemory,
                @JsonProperty("networkCost") String networkCost)
        {
            this.outputRowCount = outputRowCount;
            this.outputSize = outputSize;
            this.cpuCost = cpuCost;
            this.maxMemory = maxMemory;
            this.networkCost = networkCost;
        }

        @JsonProperty
        public String getOutputRowCount()
        {
            return outputRowCount;
        }

        @JsonProperty
        public String getOutputSize()
        {
            return outputSize;
        }

        @JsonProperty
        public String getCpuCost()
        {
            return cpuCost;
        }

        @JsonProperty
        public String getMaxMemory()
        {
            return maxMemory;
        }

        @JsonProperty
        public String getNetworkCost()
        {
            return networkCost;
        }
    }

    @Immutable
    public static class DistributedPlanStats
    {
        private final Map<String, BasicOperatorStats> operatorStats;
        private final String planNodeId;
        private final long planNodeScheduledTime;
        private final long planNodeCpuTime;
        private final long planNodeBlockedTime;
        private final long planNodeInputPositions;
        private final long planNodeInputDataSize;
        private final long planNodeOutputPositions;
        private final long planNodeOutputDataSize;
        private final long planNodeSpilledDataSize;
        private final double scheduledTimeFraction;
        private final double cpuTimeFraction;
        private final double blockedTimeFraction;

        public static DistributedPlanStats of(PlanNodeStats nodeStats, PlanRepresentation plan)
        {
            // TODO lysy: handle HashCollisionPlanNodeStats and WindowPlanNodeStats
            double scheduledTimeFraction = 100.0d * nodeStats.getPlanNodeScheduledTime().toMillis() / plan.getTotalScheduledTime().get().toMillis();
            double cpuTimeFraction = 100.0d * nodeStats.getPlanNodeCpuTime().toMillis() / plan.getTotalCpuTime().get().toMillis();
            double blockedTimeFraction = 100.0d * nodeStats.getPlanNodeBlockedTime().toMillis() / plan.getTotalBlockedTime().get().toMillis();
            return new DistributedPlanStats(
                    nodeStats.getOperatorStats(),
                    nodeStats.getPlanNodeId().toString(),
                    nodeStats.getPlanNodeScheduledTime().toMillis(),
                    nodeStats.getPlanNodeCpuTime().toMillis(),
                    nodeStats.getPlanNodeBlockedTime().toMillis(),
                    nodeStats.getPlanNodeInputPositions(),
                    nodeStats.getPlanNodeInputDataSize().toBytes(),
                    nodeStats.getPlanNodeOutputPositions(),
                    nodeStats.getPlanNodeOutputDataSize().toBytes(),
                    nodeStats.getPlanNodeSpilledDataSize().toBytes(),
                    scheduledTimeFraction,
                    cpuTimeFraction,
                    blockedTimeFraction);
        }

        @JsonCreator
        public DistributedPlanStats(
                @JsonProperty("operatorStats") Map<String, BasicOperatorStats> operatorStats,
                @JsonProperty("planNodeId") String planNodeId,
                @JsonProperty("planNodeScheduledTime") long planNodeScheduledTime,
                @JsonProperty("planNodeCpuTime") long planNodeCpuTime,
                @JsonProperty("planNodeBlockedTime") long planNodeBlockedTime,
                @JsonProperty("planNodeInputPositions") long planNodeInputPositions,
                @JsonProperty("planNodeInputDataSize") long planNodeInputDataSize,
                @JsonProperty("planNodeOutputPositions") long planNodeOutputPositions,
                @JsonProperty("planNodeOutputDataSize") long planNodeOutputDataSize,
                @JsonProperty("planNodeSpilledDataSize") long planNodeSpilledDataSize,
                @JsonProperty("scheduledTimeFraction") double scheduledTimeFraction,
                @JsonProperty("cpuTimeFraction") double cpuTimeFraction,
                @JsonProperty("blockedTimeFraction") double blockedTimeFraction)
        {
            this.operatorStats = operatorStats;
            this.planNodeId = planNodeId;
            this.planNodeScheduledTime = planNodeScheduledTime;
            this.planNodeCpuTime = planNodeCpuTime;
            this.planNodeBlockedTime = planNodeBlockedTime;
            this.planNodeInputPositions = planNodeInputPositions;
            this.planNodeInputDataSize = planNodeInputDataSize;
            this.planNodeOutputPositions = planNodeOutputPositions;
            this.planNodeOutputDataSize = planNodeOutputDataSize;
            this.planNodeSpilledDataSize = planNodeSpilledDataSize;
            this.scheduledTimeFraction = scheduledTimeFraction;
            this.cpuTimeFraction = cpuTimeFraction;
            this.blockedTimeFraction = blockedTimeFraction;
        }

        @JsonProperty
        public Map<String, BasicOperatorStats> getOperatorStats()
        {
            return operatorStats;
        }

        @JsonProperty
        public String getPlanNodeId()
        {
            return planNodeId;
        }

        @JsonProperty
        public long getPlanNodeScheduledTime()
        {
            return planNodeScheduledTime;
        }

        @JsonProperty
        public long getPlanNodeCpuTime()
        {
            return planNodeCpuTime;
        }

        @JsonProperty
        public long getPlanNodeBlockedTime()
        {
            return planNodeBlockedTime;
        }

        @JsonProperty
        public long getPlanNodeInputPositions()
        {
            return planNodeInputPositions;
        }

        @JsonProperty
        public long getPlanNodeInputDataSize()
        {
            return planNodeInputDataSize;
        }

        @JsonProperty
        public long getPlanNodeOutputPositions()
        {
            return planNodeOutputPositions;
        }

        @JsonProperty
        public long getPlanNodeOutputDataSize()
        {
            return planNodeOutputDataSize;
        }

        @JsonProperty
        public long getPlanNodeSpilledDataSize()
        {
            return planNodeSpilledDataSize;
        }

        @JsonProperty
        public double getScheduledTimeFraction()
        {
            return scheduledTimeFraction;
        }

        @JsonProperty
        public double getCpuTimeFraction()
        {
            return cpuTimeFraction;
        }

        @JsonProperty
        public double getBlockedTimeFraction()
        {
            return blockedTimeFraction;
        }
    }
}
