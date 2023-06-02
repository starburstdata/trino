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
package io.trino.cost;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.type.FixedWidthType;
import io.trino.spi.type.Type;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeProvider;
import org.pcollections.HashTreePMap;
import org.pcollections.PMap;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.util.MoreMath.firstNonNaN;
import static java.lang.Double.NaN;
import static java.lang.Double.isNaN;
import static java.util.Objects.requireNonNull;

public class PlanNodeStatsEstimate
{
    private static final double DEFAULT_DATA_SIZE_PER_COLUMN = 50;
    private static final PlanNodeStatsEstimate UNKNOWN = new PlanNodeStatsEstimate(NaN, ImmutableMap.of());

    private final RowCountEstimate outputRowCountEstimate;
    private final PMap<Symbol, SymbolStatsEstimate> symbolStatistics;

    public static PlanNodeStatsEstimate unknown()
    {
        return UNKNOWN;
    }

    public PlanNodeStatsEstimate(
            double outputRowCount,
            Map<Symbol, SymbolStatsEstimate> symbolStatistics)
    {
        this(new RowCountEstimate(outputRowCount), HashTreePMap.from(requireNonNull(symbolStatistics, "symbolStatistics is null")));
    }

    @JsonCreator
    public PlanNodeStatsEstimate(
            @JsonProperty("outputRowCountEstimate") RowCountEstimate outputRowCountEstimate,
            @JsonProperty("symbolStatistics") Map<Symbol, SymbolStatsEstimate> symbolStatistics)
    {
        this(outputRowCountEstimate, HashTreePMap.from(requireNonNull(symbolStatistics, "symbolStatistics is null")));
    }

    private PlanNodeStatsEstimate(RowCountEstimate outputRowCountEstimate, PMap<Symbol, SymbolStatsEstimate> symbolStatistics)
    {
        this.outputRowCountEstimate = requireNonNull(outputRowCountEstimate, "outputRowCountEstimate is null");
        this.symbolStatistics = requireNonNull(symbolStatistics, "symbolStatistics is null");
    }

    /**
     * Returns estimated number of rows.
     * Unknown value is represented by {@link Double#NaN}
     */
    public double getOutputRowCount()
    {
        return outputRowCountEstimate.count;
    }

    @JsonProperty
    public RowCountEstimate getOutputRowCountEstimate()
    {
        return outputRowCountEstimate;
    }

    /**
     * Returns estimated data size.
     * Unknown value is represented by {@link Double#NaN}
     */
    public double getOutputSizeInBytes(Collection<Symbol> outputSymbols, TypeProvider types)
    {
        requireNonNull(outputSymbols, "outputSymbols is null");

        return outputSymbols.stream()
                .mapToDouble(symbol -> getOutputSizeForSymbol(getSymbolStatistics(symbol), types.get(symbol)))
                .sum();
    }

    private double getOutputSizeForSymbol(SymbolStatsEstimate symbolStatistics, Type type)
    {
        checkArgument(type != null, "type is null");
        double outputRowCount = outputRowCountEstimate.count;
        double nullsFraction = firstNonNaN(symbolStatistics.getNullsFraction(), 0d);
        double numberOfNonNullRows = outputRowCount * (1.0 - nullsFraction);

        double outputSize = 0;

        // account for "is null" boolean array
        outputSize += outputRowCount;

        if (type instanceof FixedWidthType) {
            outputSize += numberOfNonNullRows * ((FixedWidthType) type).getFixedSize();
        }
        else {
            double averageRowSize = firstNonNaN(symbolStatistics.getAverageRowSize(), DEFAULT_DATA_SIZE_PER_COLUMN);
            outputSize += numberOfNonNullRows * averageRowSize;

            // account for offsets array
            outputSize += outputRowCount * Integer.BYTES;
            // TODO some types may have more overhead than just offsets array
        }

        return outputSize;
    }

    public PlanNodeStatsEstimate mapOutputRowCount(Function<Double, Double> mappingFunction)
    {
        return buildFrom(this).setOutputRowCountEstimate(outputRowCountEstimate.map(mappingFunction)).build();
    }

    public PlanNodeStatsEstimate mapSymbolColumnStatistics(Symbol symbol, Function<SymbolStatsEstimate, SymbolStatsEstimate> mappingFunction)
    {
        return buildFrom(this)
                .addSymbolStatistics(symbol, mappingFunction.apply(getSymbolStatistics(symbol)))
                .build();
    }

    public SymbolStatsEstimate getSymbolStatistics(Symbol symbol)
    {
        return symbolStatistics.getOrDefault(symbol, SymbolStatsEstimate.unknown());
    }

    @JsonProperty
    public Map<Symbol, SymbolStatsEstimate> getSymbolStatistics()
    {
        return symbolStatistics;
    }

    public Set<Symbol> getSymbolsWithKnownStatistics()
    {
        return symbolStatistics.keySet();
    }

    public boolean isOutputRowCountUnknown()
    {
        return outputRowCountEstimate.isOutputRowCountUnknown();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("outputRowCount", outputRowCountEstimate)
                .add("symbolStatistics", symbolStatistics)
                .toString();
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
        PlanNodeStatsEstimate that = (PlanNodeStatsEstimate) o;
        return Double.compare(outputRowCountEstimate.count, that.outputRowCountEstimate.count) == 0 &&
                Objects.equals(outputRowCountEstimate.confidence, that.outputRowCountEstimate.confidence) &&
                Objects.equals(symbolStatistics, that.symbolStatistics);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(outputRowCountEstimate, symbolStatistics);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder buildFrom(PlanNodeStatsEstimate other)
    {
        return new Builder(other.getOutputRowCountEstimate(), other.symbolStatistics);
    }

    public static final class Builder
    {
        private RowCountEstimate rowCountEstimate;
        private PMap<Symbol, SymbolStatsEstimate> symbolStatistics;

        public Builder()
        {
            this(RowCountEstimate.UNKNOWN, HashTreePMap.empty());
        }

        private Builder(RowCountEstimate rowCountEstimate, PMap<Symbol, SymbolStatsEstimate> symbolStatistics)
        {
            this.rowCountEstimate = rowCountEstimate;
            this.symbolStatistics = symbolStatistics;
        }

        public Builder setOutputRowCount(double outputRowCount)
        {
            this.rowCountEstimate = rowCountEstimate.map(old -> outputRowCount);
            return this;
        }

        public Builder setOutputRowCountEstimate(RowCountEstimate rowCountEstimate)
        {
            this.rowCountEstimate = requireNonNull(rowCountEstimate, "rowCountEstimate is null");
            return this;
        }

        public Builder addSymbolStatistics(Symbol symbol, SymbolStatsEstimate statistics)
        {
            symbolStatistics = symbolStatistics.plus(symbol, statistics);
            return this;
        }

        public Builder addSymbolStatistics(Map<Symbol, SymbolStatsEstimate> symbolStatistics)
        {
            this.symbolStatistics = this.symbolStatistics.plusAll(symbolStatistics);
            return this;
        }

        public Builder removeSymbolStatistics(Symbol symbol)
        {
            symbolStatistics = symbolStatistics.minus(symbol);
            return this;
        }

        public PlanNodeStatsEstimate build()
        {
            return new PlanNodeStatsEstimate(rowCountEstimate, symbolStatistics);
        }
    }

    public record RowCountEstimate(double count, EstimateConfidence confidence)
    {
        public static final RowCountEstimate UNKNOWN = new RowCountEstimate(NaN, EstimateConfidence.LOW);

        public RowCountEstimate(double outputRowCount)
        {
            this(outputRowCount, EstimateConfidence.LOW);
        }

        public RowCountEstimate(double count, EstimateConfidence confidence)
        {
            checkArgument(isNaN(count) || count >= 0, "outputRowCount cannot be negative");
            this.count = count;
            this.confidence = requireNonNull(confidence, "confidence is null");
        }

        public boolean isOutputRowCountUnknown()
        {
            return isNaN(count);
        }

        public RowCountEstimate map(Function<Double, Double> mappingFunction)
        {
            return new RowCountEstimate(mappingFunction.apply(count), confidence);
        }
    }

    public enum EstimateConfidence
    {
        HIGH, MEDIUM, LOW
    }
}
