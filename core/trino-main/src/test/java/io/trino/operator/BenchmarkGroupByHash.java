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
package io.trino.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.slice.XxHash64;
import io.trino.operator.hash.ByteArraySingleArrayBigintGroupByHash;
import io.trino.operator.hash.FastBigintGroupByHash;
import io.trino.operator.hash.VectorizedByteArraySingleArrayBigintGroupByHash;
import io.trino.operator.hash.VectorizedByteArraySingleArrayBigintGroupByHash_2;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.AbstractLongType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
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
import org.openjdk.jmh.profile.AsyncProfiler;
import org.openjdk.jmh.profile.LinuxPerfNormProfiler;
import org.openjdk.jmh.runner.RunnerException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.operator.UpdateMemory.NOOP;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(1)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkGroupByHash
{
    private static final int POSITIONS = 50_000_000;
    private static final String GROUP_COUNT_STRING = "3000000";
    private static final int GROUP_COUNT = Integer.parseInt(GROUP_COUNT_STRING);
    private static final int EXPECTED_SIZE = 10_000;
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public Object addPages(MultiChannelBenchmarkData data)
    {
        GroupByHash groupByHash = new FlatGroupByHash(data.getTypes(), data.isHashEnabled(), EXPECTED_SIZE, false, new FlatHashStrategyCompiler(TYPE_OPERATORS), NOOP);
        addInputPagesToHash(groupByHash, data.getPages());
        return groupByHash;
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public Object addPagesBigInt(MultiChannelBenchmarkData data)
    {
        GroupByHash groupByHash = new BigintGroupByHash(false, EXPECTED_SIZE, NOOP);
        addInputPagesToHash(groupByHash, data.getPages());
        return groupByHash;
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public Object addPagesFastBigint(MultiChannelBenchmarkData data)
    {
        GroupByHash groupByHash = new FastBigintGroupByHash(false, EXPECTED_SIZE, NOOP, data.stepSize);
        addInputPagesToHash(groupByHash, data.getPages());
        return groupByHash;
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public Object addPagesByteArrayBigint(MultiChannelBenchmarkData data)
    {
        GroupByHash groupByHash = new ByteArraySingleArrayBigintGroupByHash(false, EXPECTED_SIZE, NOOP);
        addInputPagesToHash(groupByHash, data.getPages());
        return groupByHash;
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public Object writeData(WriteMultiChannelBenchmarkData data)
    {
        GroupByHash groupByHash = data.getPrefilledHash();
        PageBuilder pageBuilder = new PageBuilder(POSITIONS, data.getOutputTypes());
        int[] groupIdsByPhysicalOrder = data.getGroupIdsByPhysicalOrder();
        for (int groupId : groupIdsByPhysicalOrder) {
            pageBuilder.declarePosition();
            groupByHash.appendValuesTo(groupId, pageBuilder);
            if (pageBuilder.isFull()) {
                pageBuilder.reset();
            }
        }
        return pageBuilder.build();
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public Object addPagesVectorizedByteArrayBigint(MultiChannelBenchmarkData data)
    {
        GroupByHash groupByHash = new VectorizedByteArraySingleArrayBigintGroupByHash(false, EXPECTED_SIZE, NOOP, data.stepSize);
        addInputPagesToHash(groupByHash, data.getPages());
        return groupByHash;
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public Object addPagesVectorizedByteArrayBigint2(MultiChannelBenchmarkData data)
    {
        GroupByHash groupByHash = new VectorizedByteArraySingleArrayBigintGroupByHash_2(false, EXPECTED_SIZE, NOOP, data.stepSize);
        addInputPagesToHash(groupByHash, data.getPages());
        return groupByHash;
    }

    private static void addInputPagesToHash(GroupByHash groupByHash, List<Page> pages)
    {
        for (Page page : pages) {
            Work<?> work = groupByHash.addPage(page);
            boolean finished;
            do {
                finished = work.process();
            }
            while (!finished);
        }
    }

    private static List<Page> createBigintPages(int positionCount, int groupCount, int channelCount, boolean hashEnabled, boolean useMixedBlockTypes)
    {
        List<Type> types = Collections.nCopies(channelCount, BIGINT);
        ImmutableList.Builder<Page> pages = ImmutableList.builder();
        if (hashEnabled) {
            types = ImmutableList.copyOf(Iterables.concat(types, ImmutableList.of(BIGINT)));
        }

        PageBuilder pageBuilder = new PageBuilder(types);
        int pageCount = 0;
        for (int position = 0; position < positionCount; position++) {
            int rand = ThreadLocalRandom.current().nextInt(groupCount);
            pageBuilder.declarePosition();
            for (int numChannel = 0; numChannel < channelCount; numChannel++) {
                BIGINT.writeLong(pageBuilder.getBlockBuilder(numChannel), rand);
            }
            if (hashEnabled) {
                BIGINT.writeLong(pageBuilder.getBlockBuilder(channelCount), AbstractLongType.hash(rand));
            }
            if (pageBuilder.isFull()) {
                Page page = pageBuilder.build();
                pageBuilder.reset();
                if (useMixedBlockTypes) {
                    if (pageCount % 3 == 0) {
                        pages.add(page);
                    }
                    else if (pageCount % 3 == 1) {
                        // rle page
                        Block[] blocks = new Block[page.getChannelCount()];
                        for (int channel = 0; channel < blocks.length; ++channel) {
                            blocks[channel] = RunLengthEncodedBlock.create(page.getBlock(channel).getSingleValueBlock(0), page.getPositionCount());
                        }
                        pages.add(new Page(blocks));
                    }
                    else {
                        // dictionary page
                        int[] positions = IntStream.range(0, page.getPositionCount()).toArray();
                        Block[] blocks = new Block[page.getChannelCount()];
                        for (int channel = 0; channel < page.getChannelCount(); ++channel) {
                            blocks[channel] = DictionaryBlock.create(positions.length, page.getBlock(channel), positions);
                        }
                        pages.add(new Page(blocks));
                    }
                }
                else {
                    pages.add(page);
                }
                pageCount++;
            }
        }
        pages.add(pageBuilder.build());
        return pages.build();
    }

    private static List<Page> createVarcharPages(int positionCount, int groupCount, int channelCount, boolean hashEnabled)
    {
        List<Type> types = Collections.nCopies(channelCount, VARCHAR);
        ImmutableList.Builder<Page> pages = ImmutableList.builder();
        if (hashEnabled) {
            types = ImmutableList.copyOf(Iterables.concat(types, ImmutableList.of(BIGINT)));
        }

        PageBuilder pageBuilder = new PageBuilder(types);
        for (int position = 0; position < positionCount; position++) {
            int rand = ThreadLocalRandom.current().nextInt(groupCount);
            Slice value = Slices.wrappedHeapBuffer(ByteBuffer.allocate(4).putInt(rand).flip());
            pageBuilder.declarePosition();
            for (int channel = 0; channel < channelCount; channel++) {
                VARCHAR.writeSlice(pageBuilder.getBlockBuilder(channel), value);
            }
            if (hashEnabled) {
                BIGINT.writeLong(pageBuilder.getBlockBuilder(channelCount), XxHash64.hash(value));
            }
            if (pageBuilder.isFull()) {
                pages.add(pageBuilder.build());
                pageBuilder.reset();
            }
        }
        pages.add(pageBuilder.build());
        return pages.build();
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class MultiChannelBenchmarkData
    {
        @Param("64")
        public int stepSize = 64;

        @Param({"1", "5", "10", "15", "20"})
        private int channelCount = 1;

        // todo add more group counts when JMH support programmatic ability to set OperationsPerInvocation
        @Param(GROUP_COUNT_STRING)
        private int groupCount = GROUP_COUNT;

        @Param({"true", "false"})
        private boolean hashEnabled;

        @Param({"VARCHAR", "BIGINT"})
        private String dataType = "VARCHAR";

        private List<Page> pages;
        private List<Type> types;

        @Setup
        public void setup()
        {
            switch (dataType) {
                case "VARCHAR" -> {
                    types = Collections.nCopies(channelCount, VARCHAR);
                    pages = createVarcharPages(POSITIONS, groupCount, channelCount, hashEnabled);
                }
                case "BIGINT" -> {
                    types = Collections.nCopies(channelCount, BIGINT);
                    pages = createBigintPages(POSITIONS, groupCount, channelCount, hashEnabled, false);
                }
                default -> throw new UnsupportedOperationException("Unsupported dataType");
            }
        }

        public int getChannelCount()
        {
            return channelCount;
        }

        public List<Page> getPages()
        {
            return pages;
        }

        public boolean isHashEnabled()
        {
            return hashEnabled;
        }

        public List<Type> getTypes()
        {
            return types;
        }
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class WriteMultiChannelBenchmarkData
    {
        private GroupByHash prefilledHash;
        private int[] groupIdsByPhysicalOrder;
        private List<Type> outputTypes;

        @Setup
        public void setup(MultiChannelBenchmarkData data)
        {
            prefilledHash = new FlatGroupByHash(data.getTypes(), data.isHashEnabled(), EXPECTED_SIZE, false, new FlatHashStrategyCompiler(new TypeOperators()), NOOP);
            addInputPagesToHash(prefilledHash, data.getPages());

            Integer[] groupIds = new Integer[prefilledHash.getGroupCount()];
            for (int i = 0; i < groupIds.length; i++) {
                groupIds[i] = i;
            }
            if (prefilledHash instanceof FlatGroupByHash flatGroupByHash) {
                Arrays.sort(groupIds, Comparator.comparing(flatGroupByHash::getPhysicalPosition));
            }
            groupIdsByPhysicalOrder = Arrays.stream(groupIds).mapToInt(Integer::intValue).toArray();

            outputTypes = new ArrayList<>(data.getTypes());
            if (data.isHashEnabled()) {
                outputTypes.add(BIGINT);
            }
        }

        public GroupByHash getPrefilledHash()
        {
            return prefilledHash;
        }

        public int[] getGroupIdsByPhysicalOrder()
        {
            return groupIdsByPhysicalOrder;
        }

        public List<Type> getOutputTypes()
        {
            return outputTypes;
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        // assure the benchmarks are valid before running
        MultiChannelBenchmarkData data = new MultiChannelBenchmarkData();
        data.setup();
        new BenchmarkGroupByHash().addPages(data);

        WriteMultiChannelBenchmarkData writeData = new WriteMultiChannelBenchmarkData();
        writeData.setup(data);
        new BenchmarkGroupByHash().writeData(writeData);

        benchmark(BenchmarkGroupByHash.class)
//                .includeMethod("addPages")
//                .includeMethod("addPagesBigInt")
//                .includeMethod("addPagesFastBigint")
//                .includeMethod("addPagesByteArrayBigint")
                .includeMethod("addPagesVectorizedByteArrayBigint2")
                .withOptions(optionsBuilder -> optionsBuilder
//                        .addProfiler(GCProfiler.class)
//                        .addProfiler(LinuxPerfNormProfiler.class, "events=instructions,L1-dcache-loads,L1-dcache-load-misses,L2_RQSTS.REFERENCES,L2_RQSTS.MISS,L2_RQSTS.DEMAND_DATA_RD_MISS,cycle_activity.stalls_l3_miss,L1D_PEND_MISS.FB_FULL,L1D_PEND_MISS.FB_FULL_PERIODS,L1D_PEND_MISS.L2_STALLS,L1D_PEND_MISS.PENDING,L1D_PEND_MISS.PENDING_CYCLES,l2_request.miss,cycle_activity.stalls_l3_miss,cycle_activity.stalls_l1d_miss,cycle_activity.stalls_l2_miss,cycle_activity.stalls_total,topdown.backend_bound_slots,topdown.memory_bound_slots")
//                        .addProfiler(LinuxPerfAsmProfiler.class, "tooBigThreshold=30000")
//                        .addProfiler(AsyncProfiler.class, "libPath=/Users/lukasz.stec/apps/async-profiler/async-profiler-3.0-macos/lib/libasyncProfiler.dylib")
                        .addProfiler(AsyncProfiler.class, "libPath=/home/ec2-user/async-profiler-3.0-36168a1-linux-x64/lib/libasyncProfiler.so;output=text,flamegraph;event=L1-dcache-load-misses")
                        .param("channelCount", "1")
                        .param("dataType", "BIGINT")
                        .param("hashEnabled", "true")
//                        .param("groupCount", "8000", "3000000", "10000000", "20000000")
                        .param("groupCount", "1000", "3000000", "10000000")
//                        .param("stepSize", "8", "16", "32", "64", "128", "256", "512", "1024")
                        .param("stepSize", "256")
                        .jvmArgs("-Xmx20g", "-XX:+UseTransparentHugePages"))
//                        .jvmArgs("-Xmx20g"))
                .run();
    }
}
