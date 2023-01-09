package io.trino.operator.aggregation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.trino.jmh.Benchmarks;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Type;
import io.trino.sql.tree.QualifiedName;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.CompilerControl;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.AsyncProfiler;
import org.openjdk.jmh.profile.DTraceAsmProfiler;
import org.testcontainers.shaded.org.apache.commons.io.FileUtils;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.trino.block.BlockAssertions.createRandomBlockForType;
import static io.trino.operator.PageTestUtils.Wrapping.DICTIONARY;
import static io.trino.operator.PageTestUtils.Wrapping.RUN_LENGTH;
import static io.trino.operator.PageTestUtils.createRandomPage;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static org.openjdk.jmh.annotations.CompilerControl.Mode.DONT_INLINE;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(2)
@Warmup(iterations = 10)
@Measurement(iterations = 10, time = 2)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkAggregationFunction
{
    private static final int TOTAL_ELEMENT_COUNT = 1_000_000;

    @Benchmark
    @OperationsPerInvocation(TOTAL_ELEMENT_COUNT)
    public Object generated(BenchmarkData data)
    {
        return data.processPages();
    }

    @Benchmark
    @OperationsPerInvocation(TOTAL_ELEMENT_COUNT)
    public Object manual(BenchmarkData data)
    {
        return switch (data.type) {
            case "bigint" -> data.processBigint();
            case "varchar" -> data.processVarchar();
            default -> throw new IllegalStateException("Invalid type: " + data.type);
        };
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private static final TestingFunctionResolution functionResolution = new TestingFunctionResolution();

        @Param({"max", "min"})
        private String function = "max";

        @Param({"varchar", "bigint"})
        private String type = "varchar";

        @Param({"0", "0.2"})
        private float nullRate;

        @Param({"true", "false"})
        private boolean pollute;

        private List<Page> pages;
        private Map<Type, Aggregator> aggregatorCache;

        @Setup
        public void setup(Blackhole bh)
        {
            ImmutableMap.Builder<Type, Aggregator> aggregatorCache = ImmutableMap.builder();
            for (Type type : ImmutableList.of(BIGINT, VARCHAR)) {
                TestingAggregationFunction implementation = functionResolution.getAggregateFunction(QualifiedName.of(function), fromTypes(type));
                AggregatorFactory aggregatorFactory = implementation.createAggregatorFactory(SINGLE, ImmutableList.of(0), OptionalInt.empty());
                aggregatorCache.put(type, aggregatorFactory.createAggregator());
            }
            this.aggregatorCache = aggregatorCache.buildOrThrow();
            Type type = trinoType();

            pages = ImmutableList.of(
                    new Page(createRandomBlockForType(type, TOTAL_ELEMENT_COUNT / 3, nullRate)),
                    new Page(createRandomBlockForType(type, TOTAL_ELEMENT_COUNT / 3, nullRate)),
                    new Page(createRandomBlockForType(type, TOTAL_ELEMENT_COUNT / 3, nullRate)));
            if (pollute) {
                pages = ImmutableList.of(
                        new Page(createRandomBlockForType(type, TOTAL_ELEMENT_COUNT / 3, nullRate)),
                        createRandomPage(ImmutableList.of(type), TOTAL_ELEMENT_COUNT / 3, Optional.empty(), 0, ImmutableList.of(DICTIONARY)),
                        createRandomPage(ImmutableList.of(type), TOTAL_ELEMENT_COUNT / 3, Optional.empty(), 0, ImmutableList.of(RUN_LENGTH)));
                if (bh != null) {
                    for (int i = 0; i < 5; i++) {
                        for (Type pollutionType : ImmutableList.of(BIGINT, VARCHAR)) {
                            int pollutePositionCount = 1000;
                            Aggregator aggregator = this.aggregatorCache.get(pollutionType);
                            aggregator.processPage(new Page(createRandomBlockForType(pollutionType, pollutePositionCount, 0)));
//                            aggregator.processPage(createRandomPage(ImmutableList.of(pollutionType), pollutePositionCount, Optional.empty(), 0, ImmutableList.of(DICTIONARY)));
//                            aggregator.processPage(createRandomPage(ImmutableList.of(pollutionType), pollutePositionCount, Optional.empty(), 0, ImmutableList.of(RUN_LENGTH)));
                            aggregator.processPage(new Page(createRandomBlockForType(pollutionType, pollutePositionCount, 0.2F)));
//                            aggregator.processPage(createRandomPage(ImmutableList.of(pollutionType), pollutePositionCount, Optional.empty(), 0.2F, ImmutableList.of(DICTIONARY)));
//                            aggregator.processPage(createRandomPage(ImmutableList.of(pollutionType), pollutePositionCount, Optional.empty(), 0.2F, ImmutableList.of(RUN_LENGTH)));
                            bh.consume(aggregator);
                        }
                    }
                }
            }
        }

        private Type trinoType()
        {
            return typeFromString(this.type);
        }

        private Type typeFromString(String type)
        {
            return switch (type) {
                case "bigint" -> BIGINT;
                case "int" -> INTEGER;
                case "varchar" -> VARCHAR;
                default -> throw new IllegalStateException("Invalid type: " + type);
            };
        }

        @CompilerControl(DONT_INLINE)
        public Aggregator processPages()
        {
            Aggregator aggregator = getAggregator();
            for (Page page : pages) {
                aggregator.processPage(page);
            }
            return aggregator;
        }

        private Aggregator getAggregator()
        {
            return aggregatorCache.get(trinoType());
        }

        public Object processBigint()
        {
            long max = Long.MIN_VALUE;
            for (Page page : pages) {
                Block block = page.getBlock(0);
                for (int i = 0; i < page.getPositionCount(); i++) {
                    if (!block.isNull(i)) {
                        long value = block.getLong(i, 0);
                        max = Math.max(value, max);
                    }
                }
            }
            return max;
        }

        public Object processVarcharSlow()
        {
            Slice max = EMPTY_SLICE;
            for (Page page : pages) {
                Block block = page.getBlock(0);
                for (int i = 0; i < page.getPositionCount(); i++) {
                    if (!block.isNull(i)) {
                        int length = block.getSliceLength(i);
                        Slice slice = block.getSlice(i, 0, length);

                        max = max.compareTo(slice) >= 0 ? max : slice;
                    }
                }
            }
            return max;
        }

        public Object processVarchar()
        {
            byte[] max = new byte[0];
            int maxOffset = 0;
            int maxToIndex = 0;
            for (Page page : pages) {
                Block block = page.getBlock(0);
                for (int i = 0; i < page.getPositionCount(); i++) {
                    if (!block.isNull(i)) {
                        int length = block.getSliceLength(i);
                        Slice slice = block.getSlice(i, 0, length);

                        byte[] bytes = slice.byteArray();
                        int offset = slice.byteArrayOffset();
                        int toIndex = offset + length;
                        if (Arrays.compare(max, maxOffset, maxToIndex, bytes, offset, toIndex) < 0) {
                            max = bytes;
                            maxOffset = offset;
                            maxToIndex = toIndex;
                        }
                    }
                }
            }
            return max;
        }
    }

    @Test
    public void verify()
    {
        BenchmarkData data = new BenchmarkData();
        data.setup(null);

        new BenchmarkAggregationFunction().generated(data);
        new BenchmarkAggregationFunction().manual(data);
    }

    public static void main(String[] args)
            throws Exception
    {
        // ensure the benchmarks are valid before running
        new BenchmarkAggregationFunction().verify();

        String profilerOutputDir = profilerOutputDir();
        Benchmarks.benchmark(BenchmarkAggregationFunction.class)
                .includeMethod("generated")
                .withOptions(options -> options
                                .addProfiler(AsyncProfiler.class, String.format("dir=%s;output=text;output=flamegraph", profilerOutputDir))
                                .addProfiler(DTraceAsmProfiler.class, String.format("hotThreshold=0.1;tooBigThreshold=3000;saveLog=true;saveLogTo=%s", profilerOutputDir))
//                        .param("type", "varchar", "bigint")
                                .param("type", "varchar")
                                .param("function", "max")
                                .param("nullRate", "0")
                                .param("pollute", "true")
                                .jvmArgsAppend("-XX:InlineSmallCode=470")
                                .forks(1)
                ).run();

        File dir = new File(profilerOutputDir);
        if (dir.list().length == 0) {
            FileUtils.deleteDirectory(dir);
        }
    }

    private static String profilerOutputDir()
    {
        try {
            String jmhDir = "jmh";
            new File(jmhDir).mkdirs();
            String outDir = jmhDir + "/" + String.valueOf(Files.list(Paths.get(jmhDir))
                    .flatMap(path -> {
                        try {
                            return Stream.of(Integer.parseInt(path.getFileName().toString()) + 1);
                        }
                        catch (NumberFormatException e) {
                            return Stream.empty();
                        }
                    })
                    .sorted(Comparator.reverseOrder())
                    .findFirst().orElse(0));
            new File(outDir).mkdirs();
            return outDir;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
