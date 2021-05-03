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
package io.trino.parquet.predicate;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.parquet.DictionaryPage;
import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.RichColumnDescriptor;
import io.trino.parquet.dictionary.Dictionary;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.column.statistics.BooleanStatistics;
import org.apache.parquet.column.statistics.DoubleStatistics;
import org.apache.parquet.column.statistics.FloatStatistics;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.column.statistics.LongStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.UserDefinedPredicate;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.joda.time.DateTimeZone;

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.trino.parquet.ParquetTimestampUtils.decode;
import static io.trino.parquet.predicate.PredicateUtils.isStatisticsOverflow;
import static io.trino.plugin.base.type.TrinoTimestampEncoderFactory.createTimestampEncoder;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Decimals.encodeScaledValue;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.String.format;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.Objects.requireNonNull;

public class TupleDomainParquetPredicate
        implements Predicate
{
    private final TupleDomain<ColumnDescriptor> effectivePredicate;
    private final List<RichColumnDescriptor> columns;
    private final DateTimeZone timeZone;
    private final ColumnIndexValueConverter converter;

    public TupleDomainParquetPredicate(TupleDomain<ColumnDescriptor> effectivePredicate, List<RichColumnDescriptor> columns, DateTimeZone timeZone)
    {
        this.effectivePredicate = requireNonNull(effectivePredicate, "effectivePredicate is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.timeZone = requireNonNull(timeZone, "timeZone is null");
        this.converter = new ColumnIndexValueConverter(columns);
    }

    @Override
    public boolean matches(long numberOfRows, Map<ColumnDescriptor, Statistics<?>> statistics, ParquetDataSourceId id)
            throws ParquetCorruptionException
    {
        if (numberOfRows == 0) {
            return false;
        }
        if (effectivePredicate.isNone()) {
            return false;
        }
        Map<ColumnDescriptor, Domain> effectivePredicateDomains = effectivePredicate.getDomains()
                .orElseThrow(() -> new IllegalStateException("Effective predicate other than none should have domains"));

        for (RichColumnDescriptor column : columns) {
            Domain effectivePredicateDomain = effectivePredicateDomains.get(column);
            if (effectivePredicateDomain == null) {
                continue;
            }

            Statistics<?> columnStatistics = statistics.get(column);
            if (columnStatistics == null || columnStatistics.isEmpty()) {
                // no stats for column
                continue;
            }

            Domain domain = getDomain(effectivePredicateDomain.getType(), numberOfRows, columnStatistics, id, column.toString(), timeZone);
            if (!effectivePredicateDomain.overlaps(domain)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean matches(DictionaryDescriptor dictionary)
    {
        requireNonNull(dictionary, "dictionary is null");
        if (effectivePredicate.isNone()) {
            return false;
        }
        Map<ColumnDescriptor, Domain> effectivePredicateDomains = effectivePredicate.getDomains()
                .orElseThrow(() -> new IllegalStateException("Effective predicate other than none should have domains"));

        Domain effectivePredicateDomain = effectivePredicateDomains.get(dictionary.getColumnDescriptor());

        return effectivePredicateDomain == null || effectivePredicateMatches(effectivePredicateDomain, dictionary);
    }

    @Override
    public boolean matches(long numberOfRows, ColumnIndexStore columnIndexStore, ParquetDataSourceId id)
            throws ParquetCorruptionException
    {
        requireNonNull(columnIndexStore, "columnIndexStore is null");

        if (numberOfRows == 0) {
            return false;
        }

        if (effectivePredicate.isNone()) {
            return false;
        }
        Map<ColumnDescriptor, Domain> effectivePredicateDomains = effectivePredicate.getDomains()
                .orElseThrow(() -> new IllegalStateException("Effective predicate other than none should have domains"));

        for (RichColumnDescriptor column : columns) {
            Domain effectivePredicateDomain = effectivePredicateDomains.get(column);
            if (effectivePredicateDomain == null) {
                continue;
            }

            ColumnIndex columnIndex = columnIndexStore.getColumnIndex(ColumnPath.get(column.getPath()));
            if (columnIndex == null || isEmptyColumnIndex(columnIndex)) {
                continue;
            }
            else {
                Domain domain = getDomain(effectivePredicateDomain.getType(), numberOfRows, columnIndex, id, column);
                if (effectivePredicateDomain.intersect(domain).isNone()) {
                    return false;
                }
            }
        }

        return true;
    }

    private static boolean effectivePredicateMatches(Domain effectivePredicateDomain, DictionaryDescriptor dictionary)
    {
        return effectivePredicateDomain.overlaps(getDomain(effectivePredicateDomain.getType(), dictionary));
    }

    @VisibleForTesting
    public static Domain getDomain(
            Type type,
            long rowCount,
            Statistics<?> statistics,
            ParquetDataSourceId id,
            String column,
            DateTimeZone timeZone)
            throws ParquetCorruptionException
    {
        if (statistics == null || statistics.isEmpty()) {
            return Domain.all(type);
        }

        if (statistics.isNumNullsSet() && statistics.getNumNulls() == rowCount) {
            return Domain.onlyNull(type);
        }

        boolean hasNullValue = !statistics.isNumNullsSet() || statistics.getNumNulls() != 0L;

        if (!statistics.hasNonNullValue() || statistics.genericGetMin() == null || statistics.genericGetMax() == null) {
            return Domain.create(ValueSet.all(type), hasNullValue);
        }

        if (type.equals(BOOLEAN) && statistics instanceof BooleanStatistics) {
            BooleanStatistics booleanStatistics = (BooleanStatistics) statistics;

            boolean hasTrueValues = booleanStatistics.getMin() || booleanStatistics.getMax();
            boolean hasFalseValues = !booleanStatistics.getMin() || !booleanStatistics.getMax();
            if (hasTrueValues && hasFalseValues) {
                return Domain.all(type);
            }
            if (hasTrueValues) {
                return Domain.create(ValueSet.of(type, true), hasNullValue);
            }
            if (hasFalseValues) {
                return Domain.create(ValueSet.of(type, false), hasNullValue);
            }
            // All nulls case is handled earlier
            throw new VerifyException("Impossible boolean statistics");
        }

        if ((type.equals(BIGINT) || type.equals(TINYINT) || type.equals(SMALLINT) || type.equals(INTEGER)) && (statistics instanceof LongStatistics || statistics instanceof IntStatistics)) {
            Optional<ParquetIntegerStatistics> parquetIntegerStatistics = toParquetIntegerStatistics(statistics, id, column);
            if (parquetIntegerStatistics.isEmpty() || isStatisticsOverflow(type, parquetIntegerStatistics.get())) {
                return Domain.create(ValueSet.all(type), hasNullValue);
            }
            return createDomain(type, hasNullValue, parquetIntegerStatistics.get());
        }

        if (type instanceof DecimalType && ((DecimalType) type).getScale() == 0 && (statistics instanceof LongStatistics || statistics instanceof IntStatistics)) {
            Optional<ParquetIntegerStatistics> parquetIntegerStatistics = toParquetIntegerStatistics(statistics, id, column);
            if (parquetIntegerStatistics.isEmpty() || isStatisticsOverflow(type, parquetIntegerStatistics.get())) {
                return Domain.create(ValueSet.all(type), hasNullValue);
            }
            return createDomain(type, hasNullValue, parquetIntegerStatistics.get(), statisticsValue -> {
                if (((DecimalType) type).isShort()) {
                    return statisticsValue;
                }
                return encodeScaledValue(BigDecimal.valueOf(statisticsValue), 0 /* scale */);
            });
        }

        if (type.equals(REAL) && statistics instanceof FloatStatistics) {
            FloatStatistics floatStatistics = (FloatStatistics) statistics;
            if (floatStatistics.genericGetMin() > floatStatistics.genericGetMax()) {
                throw corruptionException(column, id, floatStatistics);
            }
            if (floatStatistics.genericGetMin().isNaN() || floatStatistics.genericGetMax().isNaN()) {
                return Domain.create(ValueSet.all(type), hasNullValue);
            }

            ParquetIntegerStatistics parquetStatistics = new ParquetIntegerStatistics(
                    (long) floatToRawIntBits(floatStatistics.getMin()),
                    (long) floatToRawIntBits(floatStatistics.getMax()));
            return createDomain(type, hasNullValue, parquetStatistics);
        }

        if (type.equals(DOUBLE) && statistics instanceof DoubleStatistics) {
            DoubleStatistics doubleStatistics = (DoubleStatistics) statistics;
            if (doubleStatistics.genericGetMin() > doubleStatistics.genericGetMax()) {
                throw corruptionException(column, id, doubleStatistics);
            }
            if (doubleStatistics.genericGetMin().isNaN() || doubleStatistics.genericGetMax().isNaN()) {
                return Domain.create(ValueSet.all(type), hasNullValue);
            }

            ParquetDoubleStatistics parquetDoubleStatistics = new ParquetDoubleStatistics(doubleStatistics.genericGetMin(), doubleStatistics.genericGetMax());
            return createDomain(type, hasNullValue, parquetDoubleStatistics);
        }

        if (type instanceof VarcharType && statistics instanceof BinaryStatistics) {
            BinaryStatistics binaryStatistics = (BinaryStatistics) statistics;
            Slice minSlice = Slices.wrappedBuffer(binaryStatistics.genericGetMin().getBytes());
            Slice maxSlice = Slices.wrappedBuffer(binaryStatistics.genericGetMax().getBytes());
            if (minSlice.compareTo(maxSlice) > 0) {
                throw corruptionException(column, id, binaryStatistics);
            }
            ParquetStringStatistics parquetStringStatistics = new ParquetStringStatistics(minSlice, maxSlice);
            return createDomain(type, hasNullValue, parquetStringStatistics);
        }

        if (type.equals(DATE) && statistics instanceof IntStatistics) {
            IntStatistics intStatistics = (IntStatistics) statistics;
            if (intStatistics.genericGetMin() > intStatistics.genericGetMax()) {
                throw corruptionException(column, id, intStatistics);
            }
            ParquetIntegerStatistics parquetIntegerStatistics = new ParquetIntegerStatistics((long) intStatistics.getMin(), (long) intStatistics.getMax());
            return createDomain(type, hasNullValue, parquetIntegerStatistics);
        }

        if (type instanceof TimestampType && statistics instanceof BinaryStatistics) {
            BinaryStatistics binaryStatistics = (BinaryStatistics) statistics;
            // Parquet INT96 timestamp values were compared incorrectly for the purposes of producing statistics by older parquet writers, so
            // PARQUET-1065 deprecated them. The result is that any writer that produced stats was producing unusable incorrect values, except
            // the special case where min == max and an incorrect ordering would not be material to the result. PARQUET-1026 made binary stats
            // available and valid in that special case
            if (binaryStatistics.genericGetMin().equals(binaryStatistics.genericGetMax())) {
                return Domain.create(ValueSet.of(
                        type,
                        createTimestampEncoder((TimestampType) type, timeZone).getTimestamp(decode(binaryStatistics.genericGetMax()))),
                        hasNullValue);
            }
        }

        return Domain.create(ValueSet.all(type), hasNullValue);
    }

    @VisibleForTesting
    public Domain getDomain(Type type, long rowCount, ColumnIndex columnIndex, ParquetDataSourceId id, RichColumnDescriptor descriptor)
            throws ParquetCorruptionException
    {
        if (columnIndex == null) {
            return Domain.all(type);
        }

        String columnName = descriptor.getPrimitiveType().getName();

        if (isCorruptedColumnIndex(columnIndex)) {
            throw corruptionException(columnName, id, columnIndex);
        }

        if (isEmptyColumnIndex(columnIndex)) {
            return Domain.all(type);
        }

        long totalNullCount = columnIndex.getNullCounts().stream()
                .mapToLong(value -> value)
                .sum();
        boolean hasNullValue = totalNullCount > 0;

        if (descriptor.getType().equals(PrimitiveTypeName.INT32) || descriptor.getType().equals(PrimitiveTypeName.INT64) || descriptor.getType().equals(PrimitiveTypeName.FLOAT)) {
            List<Long> minimums = converter.getMinValuesAsLong(type, columnIndex, columnName);
            List<Long> maximums = converter.getMaxValuesAsLong(type, columnIndex, columnName);
            return createDomain(type, columnIndex, id, hasNullValue, columnName, minimums, maximums, (BiFunction<Long, Long, ParquetIntegerStatistics>) (min, max) -> new ParquetIntegerStatistics(min, max));
        }

        if (descriptor.getType().equals(PrimitiveTypeName.DOUBLE)) {
            List<Double> minimums = converter.getMinValuesAsDouble(type, columnIndex, columnName);
            List<Double> maximums = converter.getMaxValuesAsDouble(type, columnIndex, columnName);
            return createDomain(type, columnIndex, id, hasNullValue, columnName, minimums, maximums, (BiFunction<Double, Double, ParquetDoubleStatistics>) (min, max) -> new ParquetDoubleStatistics(min, max));
        }

        if (descriptor.getType().equals(PrimitiveTypeName.BINARY)) {
            List<Slice> minimums = converter.getMinValuesAsSlice(type, columnIndex);
            List<Slice> maximums = converter.getMaxValuesAsSlice(type, columnIndex);
            return createDomain(type, columnIndex, id, hasNullValue, columnName, minimums, maximums, (BiFunction<Slice, Slice, ParquetStringStatistics>) (min, max) -> new ParquetStringStatistics(min, max));
        }

        //TODO: Add INT96 and FIXED_LEN_BYTE_ARRAY later

        return Domain.create(ValueSet.all(type), hasNullValue);
    }

    private static Optional<ParquetIntegerStatistics> toParquetIntegerStatistics(Statistics<?> statistics, ParquetDataSourceId id, String column)
            throws ParquetCorruptionException
    {
        if (statistics instanceof LongStatistics) {
            LongStatistics longStatistics = (LongStatistics) statistics;
            if (longStatistics.genericGetMin() > longStatistics.genericGetMax()) {
                throw corruptionException(column, id, longStatistics);
            }
            return Optional.of(new ParquetIntegerStatistics(longStatistics.genericGetMin(), longStatistics.genericGetMax()));
        }

        if (statistics instanceof IntStatistics) {
            IntStatistics intStatistics = (IntStatistics) statistics;
            if (intStatistics.genericGetMin() > intStatistics.genericGetMax()) {
                throw corruptionException(column, id, intStatistics);
            }
            return Optional.of(new ParquetIntegerStatistics((long) intStatistics.getMin(), (long) intStatistics.getMax()));
        }

        throw new IllegalArgumentException("Cannot convert statistics of type " + statistics.getClass().getName());
    }

    @VisibleForTesting
    public static Domain getDomain(Type type, DictionaryDescriptor dictionaryDescriptor)
    {
        if (dictionaryDescriptor == null) {
            return Domain.all(type);
        }

        ColumnDescriptor columnDescriptor = dictionaryDescriptor.getColumnDescriptor();
        Optional<DictionaryPage> dictionaryPage = dictionaryDescriptor.getDictionaryPage();
        if (dictionaryPage.isEmpty()) {
            return Domain.all(type);
        }

        Dictionary dictionary;
        try {
            dictionary = dictionaryPage.get().getEncoding().initDictionary(columnDescriptor, dictionaryPage.get());
        }
        catch (Exception e) {
            // In case of exception, just continue reading the data, not using dictionary page at all
            // OK to ignore exception when reading dictionaries
            // TODO take failOnCorruptedParquetStatistics parameter and handle appropriately
            return Domain.all(type);
        }

        int dictionarySize = dictionaryPage.get().getDictionarySize();
        if (type.equals(BIGINT) && columnDescriptor.getPrimitiveType().getPrimitiveTypeName() == PrimitiveTypeName.INT64) {
            List<Long> values = new ArrayList<>(dictionarySize);
            for (int i = 0; i < dictionarySize; i++) {
                values.add(dictionary.decodeToLong(i));
            }
            return Domain.create(ValueSet.copyOf(type, values), true);
        }

        if ((type.equals(BIGINT) || type.equals(DATE)) && columnDescriptor.getPrimitiveType().getPrimitiveTypeName() == PrimitiveTypeName.INT32) {
            List<Long> values = new ArrayList<>(dictionarySize);
            for (int i = 0; i < dictionarySize; i++) {
                values.add((long) dictionary.decodeToInt(i));
            }
            return Domain.create(ValueSet.copyOf(type, values), true);
        }

        if (type.equals(DOUBLE) && columnDescriptor.getPrimitiveType().getPrimitiveTypeName() == PrimitiveTypeName.DOUBLE) {
            List<Double> values = new ArrayList<>(dictionarySize);
            for (int i = 0; i < dictionarySize; i++) {
                double value = dictionary.decodeToDouble(i);
                if (Double.isNaN(value)) {
                    return Domain.all(type);
                }
                values.add(value);
            }
            return Domain.create(ValueSet.copyOf(type, values), true);
        }

        if (type.equals(DOUBLE) && columnDescriptor.getPrimitiveType().getPrimitiveTypeName() == PrimitiveTypeName.FLOAT) {
            List<Double> values = new ArrayList<>(dictionarySize);
            for (int i = 0; i < dictionarySize; i++) {
                float value = dictionary.decodeToFloat(i);
                if (Float.isNaN(value)) {
                    return Domain.all(type);
                }
                values.add((double) value);
            }
            return Domain.create(ValueSet.copyOf(type, values), true);
        }

        if (type instanceof VarcharType && columnDescriptor.getPrimitiveType().getPrimitiveTypeName() == PrimitiveTypeName.BINARY) {
            List<Slice> values = new ArrayList<>(dictionarySize);
            for (int i = 0; i < dictionarySize; i++) {
                values.add(Slices.wrappedBuffer(dictionary.decodeToBinary(i).getBytes()));
            }
            return Domain.create(ValueSet.copyOf(type, values), true);
        }

        return Domain.all(type);
    }

    private static ParquetCorruptionException corruptionException(String column, ParquetDataSourceId id, Statistics<?> statistics)
    {
        return new ParquetCorruptionException(format("Corrupted statistics for column \"%s\" in Parquet file \"%s\": [%s]", column, id, statistics));
    }

    private static ParquetCorruptionException corruptionException(String column, ParquetDataSourceId id, ColumnIndex columnIndex)
    {
        return new ParquetCorruptionException(format("Corrupted statistics for column \"%s\" in Parquet file \"%s\". Corrupted column index: [%s]", column, id, columnIndex));
    }

    public static <T extends Comparable<T>> Domain createDomain(Type type, boolean hasNullValue, ParquetRangeStatistics<T> rangeStatistics)
    {
        return createDomain(type, hasNullValue, rangeStatistics, value -> value);
    }

    private static <T extends Comparable<T>> Domain createDomain(Type type, boolean hasNullValue, List<ParquetRangeStatistics<T>> rangeStatistics)
    {
        return createDomain(type, hasNullValue, rangeStatistics, value -> value);
    }

    private static <F, T> Domain createDomain(Type type, boolean hasNullValue, ParquetRangeStatistics<F> rangeStatistics, Function<F, T> function)
    {
        F min = rangeStatistics.getMin();
        F max = rangeStatistics.getMax();

        if (min != null && max != null) {
            return Domain.create(ValueSet.ofRanges(Range.range(type, function.apply(min), true, function.apply(max), true)), hasNullValue);
        }
        if (max != null) {
            return Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(type, function.apply(max))), hasNullValue);
        }
        if (min != null) {
            return Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(type, function.apply(min))), hasNullValue);
        }
        return Domain.create(ValueSet.all(type), hasNullValue);
    }

    // rangeStatistics is neither null nor empty, checked by the caller
    private static <F, T extends Comparable<T>> Domain createDomain(
            Type type,
            boolean hasNullValue,
            List<ParquetRangeStatistics<F>> rangeStatistics,
            Function<F, T> function)
    {
        List<Range> ranges = Lists.newArrayListWithExpectedSize(rangeStatistics.size());
        for (int i = 0; i < rangeStatistics.size(); i++) {
            F min = rangeStatistics.get(i).getMin();
            F max = rangeStatistics.get(i).getMax();
            Range range;
            if (min != null && max != null) {
                range = Range.range(type, function.apply(min), true, function.apply(max), true);
            }
            else if (max != null) {
                range = Range.lessThanOrEqual(type, function.apply(max));
            }
            else if (min != null) {
                range = Range.greaterThanOrEqual(type, function.apply(min));
            }
            else { // return early as range is unconstrained
                return Domain.create(ValueSet.all(type), hasNullValue);
            }

            ranges.add(range);
        }

        return Domain.create(ValueSet.ofRanges(ranges), hasNullValue);
    }

    private static <T extends Comparable<T>> Domain createDomain(
            Type type,
            ColumnIndex columnIndex,
            ParquetDataSourceId id,
            boolean hasNullValue,
            String columnName,
            List<T> minimums,
            List<T> maximums,
            BiFunction<? super T, ? super T, ? extends ParquetRangeStatistics<T>> statisticsCreator)
            throws ParquetCorruptionException
    {
        int pageCount = columnIndex.getMinValues().size();
        List<ParquetRangeStatistics<T>> ranges = new ArrayList<>();
        for (int i = 0; i < pageCount; i++) {
            T min = minimums.get(i);
            T max = maximums.get(i);
            if (min.compareTo(max) > 0) {
                throw corruptionException(columnName, id, columnIndex);
            }
            ranges.add(statisticsCreator.apply(min, max));
        }
        return createDomain(type, hasNullValue, ranges);
    }

    private boolean isCorruptedColumnIndex(ColumnIndex columnIndex)
    {
        if (columnIndex.getMaxValues() == null
                || columnIndex.getMinValues() == null
                || columnIndex.getNullCounts() == null
                || columnIndex.getNullPages() == null) {
            return true;
        }

        if (columnIndex.getMaxValues().size() != columnIndex.getMinValues().size()
                || columnIndex.getMaxValues().size() != columnIndex.getNullPages().size()
                || columnIndex.getMaxValues().size() != columnIndex.getNullCounts().size()) {
            return true;
        }

        return false;
    }

    // Caller should verify isCorruptedColumnIndex is false first
    private boolean isEmptyColumnIndex(ColumnIndex columnIndex)
    {
        return columnIndex.getMaxValues().size() == 0;
    }

    public FilterPredicate convertToParquetFilter()
    {
        FilterPredicate filter = null;

        for (RichColumnDescriptor column : columns) {
            Domain domain = effectivePredicate.getDomains().get().get(column);
            if (domain == null || domain.isNone()) {
                continue;
            }

            if (domain.isAll()) {
                continue;
            }

            FilterPredicate columnFilter = FilterApi.userDefined(FilterApi.intColumn(ColumnPath.get(column.getPath()).toDotString()), new DomainUserDefinedPredicate(domain));
            if (filter == null) {
                filter = columnFilter;
            }
            else {
                filter = FilterApi.or(filter, columnFilter);
            }
        }

        return filter;
    }

    /**
     * This class implements methods defined in UserDefinedPredicate based on the page statistic and tuple domain(for a column).
     */
    static class DomainUserDefinedPredicate<T extends Comparable<T>>
            extends UserDefinedPredicate<T>
            implements Serializable // Required by argument of FilterApi.userDefined call
    {
        private final Domain columnDomain;

        public DomainUserDefinedPredicate(Domain domain)
        {
            this.columnDomain = domain;
        }

        @Override
        public boolean keep(T value)
        {
            if (value == null && !columnDomain.isNullAllowed()) {
                return false;
            }

            return true;
        }

        @Override
        public boolean canDrop(org.apache.parquet.filter2.predicate.Statistics<T> statistic)
        {
            if (statistic == null) {
                return false;
            }

            if (statistic.getMin() instanceof Integer || statistic.getMin() instanceof Long) {
                Number min = (Number) statistic.getMin();
                Number max = (Number) statistic.getMax();
                return canDropCanWithRangeStats(new ParquetIntegerStatistics(min.longValue(), max.longValue()));
            }
            else if (statistic.getMin() instanceof Float) {
                Integer min = floatToRawIntBits((Float) statistic.getMin());
                Integer max = floatToRawIntBits((Float) statistic.getMax());
                return canDropCanWithRangeStats(new ParquetIntegerStatistics(min.longValue(), max.longValue()));
            }
            else if (statistic.getMin() instanceof Double) {
                Double min = (Double) statistic.getMin();
                Double max = (Double) statistic.getMax();
                return canDropCanWithRangeStats(new ParquetDoubleStatistics(min, max));
            }
            else if (statistic.getMin() instanceof Binary && columnDomain.getType() instanceof VarcharType) {
                Binary min = (Binary) statistic.getMin();
                Binary max = (Binary) statistic.getMax();
                return canDropCanWithRangeStats(new ParquetStringStatistics((Slices.wrappedBuffer(min.getBytes())), Slices.wrappedBuffer(max.getBytes())));
            }
            //TODO: Add other types

            return false;
        }

        @Override
        public boolean inverseCanDrop(org.apache.parquet.filter2.predicate.Statistics<T> statistics)
        {
            // Since we don't use LogicalNotUserDefined, this method is not called.
            // To be safe, we just keep the record by returning false.
            return false;
        }

        private boolean canDropCanWithRangeStats(ParquetRangeStatistics parquetStatistics)
        {
            Domain domain = createDomain(columnDomain.getType(), true, parquetStatistics);
            return columnDomain.intersect(domain).isNone();
        }
    }

    private class ColumnIndexValueConverter
    {
        private final Map<String, Function<Object, Object>> columnIndexConversions;

        private ColumnIndexValueConverter(List<RichColumnDescriptor> columns)
        {
            this.columnIndexConversions = new HashMap<>();
            for (RichColumnDescriptor column : columns) {
                columnIndexConversions.put(column.getPrimitiveType().getName(), getColumnIndexConversions(column.getPrimitiveType()));
            }
        }

        public List<Long> getMinValuesAsLong(Type type, ColumnIndex columnIndex, String column)
        {
            int pageCount = columnIndex.getMinValues().size();
            List<ByteBuffer> minValues = columnIndex.getMinValues();
            List<Long> minimums = Lists.newArrayListWithExpectedSize(pageCount);
            for (int i = 0; i < pageCount; i++) {
                if (TINYINT.equals(type) || SMALLINT.equals(type) || INTEGER.equals(type)) {
                    minimums.add(converter.convert(minValues.get(i), column));
                }
                else if (BIGINT.equals(type)) {
                    minimums.add(converter.convert(minValues.get(i), column));
                }
                else if (REAL.equals(type)) {
                    minimums.add((long) floatToRawIntBits(converter.convert(minValues.get(i), column)));
                }
            }
            return minimums;
        }

        public List<Long> getMaxValuesAsLong(Type type, ColumnIndex columnIndex, String column)
        {
            int pageCount = columnIndex.getMaxValues().size();
            List<ByteBuffer> maxValues = columnIndex.getMaxValues();
            List<Long> maximums = Lists.newArrayListWithExpectedSize(pageCount);
            if (TINYINT.equals(type) || SMALLINT.equals(type) || INTEGER.equals(type)) {
                for (int i = 0; i < pageCount; i++) {
                    maximums.add(converter.convert(maxValues.get(i), column));
                }
            }
            else if (BIGINT.equals(type)) {
                for (int i = 0; i < pageCount; i++) {
                    maximums.add(converter.convert(maxValues.get(i), column));
                }
            }
            else if (REAL.equals(type)) {
                for (int i = 0; i < pageCount; i++) {
                    maximums.add((long) floatToRawIntBits(converter.convert(maxValues.get(i), column)));
                }
            }
            return maximums;
        }

        public List<Double> getMinValuesAsDouble(Type type, ColumnIndex columnIndex, String column)
        {
            int pageCount = columnIndex.getMinValues().size();
            List<ByteBuffer> minValues = columnIndex.getMinValues();
            List<Double> minimums = Lists.newArrayListWithExpectedSize(pageCount);
            if (DOUBLE.equals(type)) {
                for (int i = 0; i < pageCount; i++) {
                    minimums.add(converter.convert(minValues.get(i), column));
                }
            }
            return minimums;
        }

        public List<Double> getMaxValuesAsDouble(Type type, ColumnIndex columnIndex, String column)
        {
            int pageCount = columnIndex.getMaxValues().size();
            List<ByteBuffer> maxValues = columnIndex.getMaxValues();
            List<Double> maximums = Lists.newArrayListWithExpectedSize(pageCount);
            if (DOUBLE.equals(type)) {
                for (int i = 0; i < pageCount; i++) {
                    maximums.add(converter.convert(maxValues.get(i), column));
                }
            }
            return maximums;
        }

        public List<Slice> getMinValuesAsSlice(Type type, ColumnIndex columnIndex)
        {
            int pageCount = columnIndex.getMinValues().size();
            List<ByteBuffer> minValues = columnIndex.getMinValues();
            List<Slice> minimums = Lists.newArrayListWithExpectedSize(pageCount);
            if (type instanceof VarcharType) {
                for (int i = 0; i < pageCount; i++) {
                    minimums.add(Slices.wrappedBuffer(minValues.get(i)));
                }
            }
            return minimums;
        }

        public List<Slice> getMaxValuesAsSlice(Type type, ColumnIndex columnIndex)
        {
            int pageCount = columnIndex.getMaxValues().size();
            List<ByteBuffer> maxValues = columnIndex.getMaxValues();
            List<Slice> maximums = Lists.newArrayListWithExpectedSize(pageCount);
            if (type instanceof VarcharType) {
                for (int i = 0; i < pageCount; i++) {
                    maximums.add(Slices.wrappedBuffer(maxValues.get(i)));
                }
            }
            return maximums;
        }

        private <T> T convert(ByteBuffer buf, String name)
        {
            return (T) columnIndexConversions.get(name).apply(buf);
        }

        private Function<Object, Object> getColumnIndexConversions(PrimitiveType type)
        {
            //TODO: getBoundaryOrder() is not used, should replace LITTLE_ENDIAN with getBoundaryOrder
            switch (type.getPrimitiveTypeName()) {
                case BOOLEAN:
                    return buffer -> ((ByteBuffer) buffer).get(0) != 0;
                case INT32:
                    return buffer -> ((ByteBuffer) buffer).order(LITTLE_ENDIAN).getInt(0);
                case INT64:
                    return buffer -> ((ByteBuffer) buffer).order(LITTLE_ENDIAN).getLong(0);
                case FLOAT:
                    return buffer -> ((ByteBuffer) buffer).order(LITTLE_ENDIAN).getFloat(0);
                case DOUBLE:
                    return buffer -> ((ByteBuffer) buffer).order(LITTLE_ENDIAN).getDouble(0);
                case BINARY:
                case FIXED_LEN_BYTE_ARRAY:
                case INT96:
                    // TODO: return buffer -> Binary.fromReusedByteBuffer((ByteBuffer) buffer);
                    return binary -> ByteBuffer.wrap(((Binary) binary).getBytes());
                default:
            }

            return obj -> obj;
        }
    }
}
