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
package io.prestosql.execution.buffer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.OutputStreamSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.BlockEncoding;
import io.prestosql.spi.block.Int128ArrayBlockEncoding;
import io.prestosql.spi.block.IntArrayBlockEncoding;
import io.prestosql.spi.block.LongArrayBlockEncoding;
import io.prestosql.spi.block.VariableWidthBlockEncoding;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Decimals;
import io.prestosql.spi.type.SqlDecimal;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static com.google.common.io.Files.createTempDir;
import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.execution.buffer.PagesSerdeUtil.readPages;
import static io.prestosql.execution.buffer.PagesSerdeUtil.writePages;
import static io.prestosql.plugin.tpch.TpchTables.getTablePages;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.spi.type.Varchars.truncateToLength;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(3)
@Warmup(iterations = 30, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 20, time = 500, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
@OperationsPerInvocation(BenchmarkPagesSerde.ROWS)
public class BenchmarkPagesSerde
{
    private static final DecimalType LONG_DECIMAL_TYPE = createDecimalType(30, 5);

    public static final int ROWS = 10_000_000;
    private static final int MAX_STRING = 19;

    @Benchmark
    public Object readLongDecimalNoNull(LongDecimalNoNullBenchmarkData data)
    {
        return ImmutableList.copyOf(readPages(data.getPagesSerde(), new BasicSliceInput(data.getDataSource())));
    }

    @Benchmark
    public Object readLongDecimalWithNull(LongDecimalWithNullBenchmarkData data)
    {
        return ImmutableList.copyOf(readPages(data.getPagesSerde(), new BasicSliceInput(data.getDataSource())));
    }

    @Benchmark
    public Object readLongNonRandomNoNull(BigintNonRandomNoNullNullBenchmarkData data)
    {
        return ImmutableList.copyOf(readPages(data.getPagesSerde(), new BasicSliceInput(data.getDataSource())));
    }

    @Benchmark
    public Object readLongNoNull(BigintNoNullBenchmarkData data)
    {
        return ImmutableList.copyOf(readPages(data.getPagesSerde(), new BasicSliceInput(data.getDataSource())));
    }

    @Benchmark
    public Object readLongWithNull(BigintWithNullBenchmarkData data)
    {
        return ImmutableList.copyOf(readPages(data.getPagesSerde(), new BasicSliceInput(data.getDataSource())));
    }

    @Benchmark
    public Object readLongWithNullNonOptimized(BigintWithNullBenchmarkDataNonOptimized data)
    {
        return ImmutableList.copyOf(readPages(data.getPagesSerde(), new BasicSliceInput(data.getDataSource())));
    }

    @Benchmark
    public Object readLongWithNullAndAggregate(BigintWithNullBenchmarkData data)
    {
        List<Page> pages = ImmutableList.copyOf(readPages(data.getPagesSerde(), new BasicSliceInput(data.getDataSource())));
        long sum = 0;
        for (Page page : pages) {
            Block block = page.getBlock(0);
            for (int i = 0; i < block.getPositionCount(); ++i) {
                if (!block.isNull(i)) {
                    sum += BIGINT.getLong(block, i);
                }
            }
        }
        return sum;
    }

    @Benchmark
    public Object readLongWithNullAndAggregateFixedWidthBlock(BigintWithNullBenchmarkDataFixedWidthBlock data)
    {
        List<Page> pages = ImmutableList.copyOf(readPages(data.getPagesSerde(), new BasicSliceInput(data.getDataSource())));
        long sum = 0;
        for (Page page : pages) {
            Block block = page.getBlock(0);
            for (int i = 0; i < block.getPositionCount(); ++i) {
                if (!block.isNull(i)) {
                    sum += BIGINT.getLong(block, i);
                }
            }
        }
        return sum;
    }

    @Benchmark
    public Object readSliceDirectNoNull(VarcharDirectNoNullBenchmarkData data)
    {
        return ImmutableList.copyOf(readPages(data.getPagesSerde(), new BasicSliceInput(data.getDataSource())));
    }

    @Benchmark
    public Object readSliceDirectWithNull(VarcharDirectWithNullBenchmarkData data)
    {
        return ImmutableList.copyOf(readPages(data.getPagesSerde(), new BasicSliceInput(data.getDataSource())));
    }

    @Benchmark
    public Object readSliceDirectWithNullAndCopy(VarcharDirectWithNullBenchmarkData data)
    {
        return Streams.stream(readPages(data.getPagesSerde(), new BasicSliceInput(data.getDataSource())))
                .map(page -> {
                    Block block = page.getBlock(0);
                    return block.copyRegion(0, block.getPositionCount());
                })
                .collect(ImmutableList.toImmutableList());
    }

    @Benchmark
    public Object readLineitem(LineitemBenchmarkData data)
    {
        return ImmutableList.copyOf(readPages(data.getPagesSerde(), new BasicSliceInput(data.getDataSource())));
    }

    public abstract static class BenchmarkData
    {
        protected final Random random = new Random(0);
        private File temporaryDirectory;
        private File file;
        private Slice dataSource;
        private PagesSerde pagesSerde;

        public void setup(Map<String, BlockEncoding> blockEncodings, Iterator<Page> pages)
                throws Exception
        {
            temporaryDirectory = createTempDir();
            file = new File(temporaryDirectory, randomUUID().toString());
            pagesSerde = new TestingPagesSerdeFactory(new TestingBlockEncodingSerde(blockEncodings::get), false).createPagesSerde();

            writePages(pagesSerde, new OutputStreamSliceOutput(new FileOutputStream(file)), pages);
            dataSource = Slices.mapFileReadOnly(file);

            System.err.println(this.getClass().getSimpleName() + ", file length " + file.length());
        }

        public void setup(Type type, BlockEncoding blockEncoding, Iterator<?> values)
                throws Exception
        {
            temporaryDirectory = createTempDir();
            file = new File(temporaryDirectory, randomUUID().toString());
            pagesSerde = new TestingPagesSerdeFactory(new TestingBlockEncodingSerde(ignored -> blockEncoding), false).createPagesSerde();
            PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(type));
            BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(0);
            ImmutableList.Builder<Page> pages = ImmutableList.builder();
            while (values.hasNext()) {
                Object value = values.next();
                if (value == null) {
                    blockBuilder.appendNull();
                }
                else if (BIGINT.equals(type)) {
                    BIGINT.writeLong(blockBuilder, ((Number) value).longValue());
                }
                else if (Decimals.isLongDecimal(type)) {
                    type.writeSlice(blockBuilder, Decimals.encodeUnscaledValue(((SqlDecimal) value).toBigDecimal().unscaledValue()));
                }
                else if (type instanceof VarcharType) {
                    Slice slice = truncateToLength(utf8Slice((String) value), type);
                    type.writeSlice(blockBuilder, slice);
                }
                else {
                    throw new IllegalArgumentException("Unsupported type " + type);
                }
                pageBuilder.declarePosition();
                if (pageBuilder.isFull()) {
                    pages.add(pageBuilder.build());
                    pageBuilder.reset();
                    blockBuilder = pageBuilder.getBlockBuilder(0);
                }
            }
            if (pageBuilder.getPositionCount() > 0) {
                pages.add(pageBuilder.build());
            }
            writePages(pagesSerde, new OutputStreamSliceOutput(new FileOutputStream(file)), pages.build().iterator());
            dataSource = Slices.mapFileReadOnly(file);

            System.err.println(this.getClass().getSimpleName() + ", file length " + file.length());
        }

        public PagesSerde getPagesSerde()
        {
            return pagesSerde;
        }

        public Slice getDataSource()
        {
            return dataSource;
        }
    }

    @State(Scope.Thread)
    public static class LongDecimalNoNullBenchmarkData
            extends BenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(LONG_DECIMAL_TYPE, new OptimizedInt128ArrayBlockEncoding(), createValues());
        }

        private Iterator<?> createValues()
        {
            List<SqlDecimal> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                values.add(new SqlDecimal(new BigInteger(96, random), 30, 5));
            }
            return values.iterator();
        }
    }

    @State(Scope.Thread)
    public static class LongDecimalWithNullBenchmarkData
            extends BenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(LONG_DECIMAL_TYPE, new OptimizedInt128ArrayBlockEncoding(), createValues());
        }

        private Iterator<?> createValues()
        {
            List<SqlDecimal> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                if (random.nextBoolean()) {
                    values.add(new SqlDecimal(new BigInteger(96, random), 30, 5));
                }
                else {
                    values.add(null);
                }
            }
            return values.iterator();
        }
    }

    @State(Scope.Thread)
    public static class BigintNonRandomNoNullNullBenchmarkData
            extends BenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(BIGINT, new OptimizedLongArrayBlockEncoding(), createValues());
        }

        private Iterator<?> createValues()
        {
            List<Long> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                values.add((long) i);
            }
            return values.iterator();
        }
    }

    @State(Scope.Thread)
    public static class BigintNoNullBenchmarkData
            extends BenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(BIGINT, new OptimizedLongArrayBlockEncoding(), createValues());
        }

        private Iterator<?> createValues()
        {
            List<Long> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                values.add(random.nextLong());
            }
            return values.iterator();
        }
    }

    @State(Scope.Thread)
    public static class BigintWithNullBenchmarkData
            extends BenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(BIGINT, new OptimizedLongArrayBlockEncoding(), createValues());
        }

        private Iterator<?> createValues()
        {
            List<Long> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                if (random.nextBoolean()) {
                    values.add(random.nextLong());
                }
                else {
                    values.add(null);
                }
            }
            return values.iterator();
        }
    }

    @State(Scope.Thread)
    public static class BigintWithNullBenchmarkDataNonOptimized
            extends BenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(BIGINT, new LongArrayBlockEncoding(), createValues());
        }

        private Iterator<?> createValues()
        {
            List<Long> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                if (random.nextBoolean()) {
                    values.add(random.nextLong());
                }
                else {
                    values.add(null);
                }
            }
            return values.iterator();
        }
    }

    @State(Scope.Thread)
    public static class BigintWithNullBenchmarkDataFixedWidthBlock
            extends BenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(BIGINT, new LongArrayBlockToFixedWidthBlockEncoding(), createValues());
        }

        private Iterator<?> createValues()
        {
            List<Long> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                if (random.nextBoolean()) {
                    values.add(random.nextLong());
                }
                else {
                    values.add(null);
                }
            }
            return values.iterator();
        }
    }

    @State(Scope.Thread)
    public static class VarcharDirectNoNullBenchmarkData
            extends BenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(VARCHAR, new VariableWidthBlockEncoding(), createValues());
        }

        private Iterator<?> createValues()
        {
            List<String> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                values.add(randomAsciiString(random));
            }
            return values.iterator();
        }
    }

    @State(Scope.Thread)
    public static class VarcharDirectWithNullBenchmarkData
            extends BenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(VARCHAR, new VariableWidthBlockEncoding(), createValues());
        }

        private Iterator<?> createValues()
        {
            List<String> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                if (random.nextBoolean()) {
                    values.add(randomAsciiString(random));
                }
                else {
                    values.add(null);
                }
            }
            return values.iterator();
        }
    }

    @State(Scope.Thread)
    public static class LineitemBenchmarkData
            extends BenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(
                    ImmutableMap.<String, BlockEncoding>builder()
                            .put(LongArrayBlockEncoding.NAME, new OptimizedLongArrayBlockEncoding())
                            .put(IntArrayBlockEncoding.NAME, new OptimizedIntArrayBlockEncoding())
                            .put(Int128ArrayBlockEncoding.NAME, new OptimizedInt128ArrayBlockEncoding())
                            .put(VariableWidthBlockEncoding.NAME, new VariableWidthBlockEncoding())
                            .build(),
                    getTablePages("lineitem", 0.1, 8 * 1024));
        }
    }

    private static String randomAsciiString(Random random)
    {
        char[] value = new char[random.nextInt(MAX_STRING)];
        for (int i = 0; i < value.length; i++) {
            value[i] = (char) random.nextInt(Byte.MAX_VALUE);
        }
        return new String(value);
    }

    @Test
    public void test()
            throws Exception
    {
        BenchmarkPagesSerde benchmark = new BenchmarkPagesSerde();
        LineitemBenchmarkData data = new LineitemBenchmarkData();
        data.setup();
        Object obj = null;
        for (int i = 0; i < 1_000_000; ++i) {
            obj = benchmark.readLineitem(data);
        }
        System.err.println(obj.toString());
    }

    public static void main(String[] args)
            throws Exception
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkPagesSerde.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
