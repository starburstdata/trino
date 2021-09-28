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
package io.trino.operator.join;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import io.airlift.units.DataSize;
import io.trino.RowPagesBuilder;
import io.trino.Session;
import io.trino.client.ClientCapabilities;
import io.trino.execution.Lifespan;
import io.trino.operator.DriverContext;
import io.trino.operator.InterpretedHashGenerator;
import io.trino.operator.Operator;
import io.trino.operator.OperatorFactories;
import io.trino.operator.OperatorFactory;
import io.trino.operator.PagesIndex;
import io.trino.operator.PartitionFunction;
import io.trino.operator.TaskContext;
import io.trino.operator.TrinoOperatorFactories;
import io.trino.operator.exchange.LocalExchangeMemoryManager;
import io.trino.operator.exchange.LocalPartitionGenerator;
import io.trino.operator.exchange.PageReference;
import io.trino.operator.exchange.PartitioningExchanger;
import io.trino.operator.join.HashBuilderOperator.HashBuilderOperatorFactory;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spiller.SingleStreamSpillerFactory;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.TestingTaskContext;
import io.trino.type.BlockTypeOperators;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.options.TimeValue;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.trino.RowPagesBuilder.rowPagesBuilder;
import static io.trino.SystemSessionProperties.BUILD_HASH_THREAD_COUNT;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.operator.join.JoinBridgeManager.lookupAllAtOnce;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spiller.PartitioningSpillerFactory.unsupportedPartitioningSpillerFactory;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Arrays.stream;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Thread;

@SuppressWarnings("MethodMayBeStatic")
@State(Thread)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(3)
@Warmup(iterations = 5)
@Measurement(iterations = 10, time = 2, timeUnit = SECONDS)
public class BenchmarkHashBuildAndJoinOperators
{
    private static final int HASH_BUILD_OPERATOR_ID = 1;
    private static final int HASH_JOIN_OPERATOR_ID = 2;
    private static final PlanNodeId TEST_PLAN_NODE_ID = new PlanNodeId("test");
    private static final BlockTypeOperators TYPE_OPERATOR_FACTORY = new BlockTypeOperators(new TypeOperators());

    @State(Thread)
    public static class BuildContext
    {
        protected static final int ROWS_PER_PAGE = 1024;
        protected static final int BUILD_ROWS_NUMBER = 8_000_000;

        @Param({"varchar", "bigint", "all"})
        protected String hashColumns = "bigint";

        @Param({"false", "true"})
        protected boolean buildHashEnabled;

        @Param({"1", "5"})
        protected int buildRowsRepetition = 1;

        @Param({"1", "2", "4", "6", "8"})
        protected int threadCount;

        @Param({"false", "true"})
        protected boolean partitioned;

        @Param({"false", "true"})
        protected boolean sortChannel;

        protected ExecutorService executor;
        protected ScheduledExecutorService scheduledExecutor;
        protected List<Page> buildPages;
        protected OptionalInt hashChannel;
        protected List<Type> types;
        protected List<Integer> hashChannels;

        @Setup
        public void setup()
        {
            switch (hashColumns) {
                case "varchar":
                    hashChannels = Ints.asList(0);
                    break;
                case "bigint":
                    hashChannels = Ints.asList(1);
                    break;
                case "all":
                    hashChannels = Ints.asList(0, 1, 2);
                    break;
                default:
                    throw new UnsupportedOperationException(format("Unknown hashColumns value [%s]", hashColumns));
            }
            executor = Executors.newFixedThreadPool(threadCount, daemonThreadsNamed(getClass().getSimpleName() + "-executor-%s"));
            scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));

            initializeBuildPages();
        }

        @TearDown
        public void tearDown()
        {
            executor.shutdownNow();
            scheduledExecutor.shutdownNow();
        }

        public TaskContext createTaskContext()
        {
            return TestingTaskContext.createTaskContext(executor, scheduledExecutor, getSession(), DataSize.of(50, GIGABYTE));
        }

        private Session getSession()
        {
            return testSessionBuilder()
                    .setCatalog("tpch")
                    .setSchema(TINY_SCHEMA_NAME)
                    .setClientCapabilities(stream(ClientCapabilities.values())
                            .map(ClientCapabilities::toString)
                            .collect(toImmutableSet()))
                    .setSystemProperty(BUILD_HASH_THREAD_COUNT, String.valueOf(partitioned ? 1 : threadCount))
                    .build();
        }

        public OptionalInt getHashChannel()
        {
            return hashChannel;
        }

        public List<Integer> getHashChannels()
        {
            return hashChannels;
        }

        public List<Type> getTypes()
        {
            return types;
        }

        public List<Page> getBuildPages()
        {
            return buildPages;
        }

        protected void initializeBuildPages()
        {
            RowPagesBuilder buildPagesBuilder = rowPagesBuilder(buildHashEnabled, hashChannels, ImmutableList.of(BIGINT, BIGINT, BIGINT));

            int maxValue = BUILD_ROWS_NUMBER / buildRowsRepetition + 40;
            int rows = 0;
            while (rows < BUILD_ROWS_NUMBER) {
                int newRows = Math.min(BUILD_ROWS_NUMBER - rows, ROWS_PER_PAGE);
                buildPagesBuilder.addSequencePage(newRows, (rows + 20) % maxValue, (rows + 30) % maxValue, (rows + 40) % maxValue);
                buildPagesBuilder.pageBreak();
                rows += newRows;
            }

            types = buildPagesBuilder.getTypes();
            buildPages = buildPagesBuilder.build();
            hashChannel = buildPagesBuilder.getHashChannel()
                    .map(OptionalInt::of).orElse(OptionalInt.empty());
        }
    }

    @State(Thread)
    public static class JoinContext
            extends BuildContext
    {
        protected static final int PROBE_ROWS_NUMBER = 1_400_000;

        @Param({"0.1", "1", "2"})
        protected double matchRate = 1;

        @Param({"bigint", "all"})
        protected String outputColumns = "bigint";

        protected List<Page> probePages;
        protected List<Integer> outputChannels;

        protected OperatorFactory joinOperatorFactory;

        @Override
        @Setup
        public void setup()
        {
            setup(new TrinoOperatorFactories());
        }

        public void setup(OperatorFactories operatorFactories)
        {
            super.setup();

            switch (outputColumns) {
                case "varchar":
                    outputChannels = Ints.asList(0);
                    break;
                case "bigint":
                    outputChannels = Ints.asList(1);
                    break;
                case "all":
                    outputChannels = Ints.asList(0, 1, 2);
                    break;
                default:
                    throw new UnsupportedOperationException(format("Unknown outputColumns value [%s]", hashColumns));
            }

            JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = getLookupSourceFactoryManager(this, outputChannels, partitioned ? threadCount : 1);
            joinOperatorFactory = operatorFactories.innerJoin(
                    HASH_JOIN_OPERATOR_ID,
                    TEST_PLAN_NODE_ID,
                    lookupSourceFactory,
                    false,
                    false,
                    false,
                    types,
                    hashChannels,
                    hashChannel,
                    Optional.of(outputChannels),
                    OptionalInt.empty(),
                    unsupportedPartitioningSpillerFactory(),
                    TYPE_OPERATOR_FACTORY);
            buildHash(this, lookupSourceFactory, outputChannels, partitioned ? threadCount : 1);
            initializeProbePages();
        }

        @TearDown
        @Override
        public void tearDown()
        {
            super.tearDown();
        }

        public OperatorFactory getJoinOperatorFactory()
        {
            return joinOperatorFactory;
        }

        public List<Page> getProbePages()
        {
            return probePages;
        }

        protected void initializeProbePages()
        {
            RowPagesBuilder probePagesBuilder = rowPagesBuilder(buildHashEnabled, hashChannels, ImmutableList.of(VARCHAR, BIGINT, BIGINT));

            Random random = new Random(42);
            int remainingRows = PROBE_ROWS_NUMBER;
            int rowsInPage = 0;
            while (remainingRows > 0) {
                double roll = random.nextDouble();

                int columnA = 20 + remainingRows;
                int columnB = 30 + remainingRows;
                int columnC = 40 + remainingRows;

                int rowsCount = 1;
                if (matchRate < 1) {
                    // each row has matchRate chance to join
                    if (roll > matchRate) {
                        // generate not matched row
                        columnA *= -1;
                        columnB *= -1;
                        columnC *= -1;
                    }
                }
                else if (matchRate > 1) {
                    // each row has will be repeated between one and 2*matchRate times
                    roll = roll * 2 * matchRate + 1;
                    // example for matchRate == 2:
                    // roll is within [0, 5) range
                    // rowsCount is within [0, 4] range, where each value has same probability
                    // so expected rowsCount is 2
                    rowsCount = (int) Math.floor(roll);
                }

                for (int i = 0; i < rowsCount; i++) {
                    if (rowsInPage >= ROWS_PER_PAGE) {
                        probePagesBuilder.pageBreak();
                        rowsInPage = 0;
                    }
                    probePagesBuilder.row(format("%d", columnA), columnB, columnC);
                    --remainingRows;
                    rowsInPage++;
                }
            }
            probePages = probePagesBuilder.build();
        }
    }

    @Benchmark
    public JoinBridgeManager<PartitionedLookupSourceFactory> benchmarkBuildHash(BuildContext buildContext)
    {
        List<Integer> outputChannels = ImmutableList.of(0, 1, 2);
        int partitionCount = buildContext.partitioned ? buildContext.threadCount : 1;
        JoinBridgeManager<PartitionedLookupSourceFactory> joinBridgeManager = getLookupSourceFactoryManager(buildContext, outputChannels, partitionCount);
        buildHash(buildContext, joinBridgeManager, outputChannels, partitionCount);
        return joinBridgeManager;
    }

    private static JoinBridgeManager<PartitionedLookupSourceFactory> getLookupSourceFactoryManager(BuildContext buildContext, List<Integer> outputChannels, int partitionCount)
    {
        return lookupAllAtOnce(new PartitionedLookupSourceFactory(
                buildContext.getTypes(),
                outputChannels.stream()
                        .map(buildContext.getTypes()::get)
                        .collect(toImmutableList()),
                buildContext.getHashChannels().stream()
                        .map(buildContext.getTypes()::get)
                        .collect(toImmutableList()),
                partitionCount,
                false,
                TYPE_OPERATOR_FACTORY));
    }

    private static void buildHash(BuildContext buildContext, JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager, List<Integer> outputChannels, int partitionCount)
    {
        HashBuilderOperatorFactory hashBuilderOperatorFactory = new HashBuilderOperatorFactory(
                HASH_BUILD_OPERATOR_ID,
                TEST_PLAN_NODE_ID,
                lookupSourceFactoryManager,
                outputChannels,
                buildContext.getHashChannels(),
                buildContext.getHashChannel(),
                buildContext.sortChannel ? Optional.of((session, addresses, pages) -> (leftPosition, rightPosition, rightPage) -> true) : Optional.empty(),
                buildContext.sortChannel ? Optional.of(0) : Optional.empty(),
                buildContext.sortChannel ?
                        ImmutableList.of((session, addresses, pages) -> (leftPosition, rightPosition, rightPage) -> false) :
                        ImmutableList.of(),
                10_000,
                new PagesIndex.TestingFactory(false),
                false,
                SingleStreamSpillerFactory.unsupportedSingleStreamSpillerFactory());

        Operator[] operators = IntStream.range(0, partitionCount)
                .mapToObj(i -> buildContext.createTaskContext()
                        .addPipelineContext(0, true, true, partitionCount > 1)
                        .addDriverContext())
                .map(hashBuilderOperatorFactory::createOperator)
                .toArray(Operator[]::new);

        if (partitionCount == 1) {
            for (Page page : buildContext.getBuildPages()) {
                operators[0].addInput(page);
            }
        }
        else {
            addInputPartitioned(buildContext, operators);
        }
        LookupSourceFactory lookupSourceFactory = lookupSourceFactoryManager.getJoinBridge(Lifespan.taskWide());
        ListenableFuture<LookupSourceProvider> lookupSourceProvider = lookupSourceFactory.createLookupSourceProvider();
        List<? extends Future<?>> futures = Arrays.stream(operators).map(operator -> buildContext.executor.submit(operator::finish))
                .collect(toImmutableList());
        futures.forEach(Futures::getUnchecked);
        if (!lookupSourceProvider.isDone()) {
            throw new AssertionError("Expected lookup source provider to be ready");
        }
        getFutureValue(lookupSourceProvider).close();
    }

    private static void addInputPartitioned(BuildContext buildContext, Operator[] operators)
    {
        int partitionCount = operators.length;
        PartitionFunction partitionGenerator = new LocalPartitionGenerator(
                new InterpretedHashGenerator(
                        buildContext.getHashChannels().stream()
                                .map(channel -> buildContext.getTypes().get(channel))
                                .collect(toImmutableList()),
                        buildContext.getHashChannels(),
                        TYPE_OPERATOR_FACTORY),
                partitionCount);

        List<BlockingQueue<PageReference>> queues = IntStream.range(0, partitionCount)
                .mapToObj(i -> new ArrayBlockingQueue<PageReference>(buildContext.getBuildPages().size())).collect(toImmutableList());
        List<Consumer<PageReference>> partitionConsumers = IntStream.range(0, partitionCount)
                .mapToObj(i -> (Consumer<PageReference>) pageReference -> {
                    try {
                        queues.get(i).put(pageReference);
                    }
                    catch (InterruptedException e) {
                        java.lang.Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                }).collect(toImmutableList());

        AtomicBoolean done = new AtomicBoolean(false);
        // setup reading threads
        IntStream.range(0, partitionCount)
                .forEach(i -> {
                    Operator operator = operators[i];
                    buildContext.executor.submit(() -> {
                        BlockingQueue<PageReference> queue = queues.get(i);
                        while (!done.get()) {
                            try {
                                PageReference pageReference = queue.poll(10, MILLISECONDS);
                                if (pageReference != null) {
                                    operator.addInput(pageReference.removePage());
                                }
                            }
                            catch (InterruptedException e) {
                                java.lang.Thread.currentThread().interrupt();
                                throw new RuntimeException(e);
                            }
                        }
                    });
                });
        LocalExchangeMemoryManager memoryManager = new LocalExchangeMemoryManager(Long.MAX_VALUE);
        PartitioningExchanger partitioningExchanger = new PartitioningExchanger(partitionConsumers, memoryManager, Function.identity(), partitionGenerator);
        for (Page page : buildContext.getBuildPages()) {
            partitioningExchanger.accept(page);
        }
        while (!queues.stream().allMatch(Queue::isEmpty)) {
            Uninterruptibles.sleepUninterruptibly(Duration.ofMillis(1));
        }
        done.set(true);
    }

    private static Page[] partitionPages(Page page, List<Type> types, int partitionCount, PartitionFunction partitionGenerator)
    {
        PageBuilder[] builders = new PageBuilder[partitionCount];

        for (int i = 0; i < partitionCount; i++) {
            builders[i] = new PageBuilder(types);
        }

        for (int i = 0; i < page.getPositionCount(); i++) {
            int partition = partitionGenerator.getPartition(page, i);
            appendRow(builders[partition], types, page, i);
        }

        return Arrays.stream(builders)
                .map(PageBuilder::build)
                .toArray(Page[]::new);
    }

    private static void appendRow(PageBuilder pageBuilder, List<Type> types, Page page, int position)
    {
        pageBuilder.declarePosition();

        for (int channel = 0; channel < types.size(); channel++) {
            Type type = types.get(channel);
            type.appendTo(page.getBlock(channel), position, pageBuilder.getBlockBuilder(channel));
        }
    }

    @Benchmark
    public List<Page> benchmarkJoinHash(JoinContext joinContext)
            throws Exception
    {
        DriverContext driverContext = joinContext.createTaskContext().addPipelineContext(0, true, true, false).addDriverContext();
        Operator joinOperator = joinContext.getJoinOperatorFactory().createOperator(driverContext);

        Iterator<Page> input = joinContext.getProbePages().iterator();
        ImmutableList.Builder<Page> outputPages = ImmutableList.builder();

        boolean finishing = false;
        for (int loops = 0; !joinOperator.isFinished() && loops < 1_000_000; loops++) {
            if (joinOperator.needsInput()) {
                if (input.hasNext()) {
                    Page inputPage = input.next();
                    joinOperator.addInput(inputPage);
                }
                else if (!finishing) {
                    joinOperator.finish();
                    finishing = true;
                }
            }

            Page outputPage = joinOperator.getOutput();
            if (outputPage != null) {
                outputPages.add(outputPage);
            }
        }

        joinOperator.close();

        return outputPages.build();
    }

    @Test
    public void testBenchmarkJoinHash()
            throws Exception
    {
        JoinContext joinContext = new JoinContext();
        joinContext.setup();
        benchmarkJoinHash(joinContext);
        // ensure that build side hash table is not freed
        List<Page> pages = benchmarkJoinHash(joinContext);

        // assert that there are any rows
        checkState(!pages.isEmpty());
        checkState(pages.get(0).getPositionCount() > 0);
    }

    @Test
    public void testBenchmarkBuildHash()
    {
        BuildContext buildContext = new BuildContext();
        buildContext.setup();
        benchmarkBuildHash(buildContext);
    }

    public static void main(String[] args)
            throws Exception
    {
        benchmark(BenchmarkHashBuildAndJoinOperators.class)
                .withOptions(options -> options
//                                .addProfiler(AsyncProfiler.class, asyncProfilerProperties())
                                .forks(1)
                                .warmupTime(TimeValue.seconds(1))
                                .measurementTime(TimeValue.seconds(1))
                                .param("buildHashEnabled", "true")
//                                .param("buildRowsRepetition", "1", "10", "100", "1000", "1000000")
                                .param("buildRowsRepetition", "1", "100")
                                .param("matchRate", "1")
                                .param("hashColumns", "bigint")
                                .param("outputColumns", "bigint")
//                                .param("partitionCount", "1")
//                                .param("threadCount", "1", "2", "4", "8")
                                .param("threadCount", "1", "4", "8")
                                .param("sortChannel", "false")
//                                .param("partitioned", "true")
//                                .exclude(".*benchmarkBuildHash.*")
                                .exclude(".*benchmarkJoinHash.*")
//                        .jvmArgsAppend("-XX:+UnlockDiagnosticVMOptions", "-XX:+TraceClassLoading", "-XX:+LogCompilation", "-XX:+DebugNonSafepoints", "-XX:+PrintAssembly", "-XX:+PrintInlining")
                )
                .run();
    }

    private static String asyncProfilerProperties()
    {
        String jmhDir = "jmh";
        new File(jmhDir).mkdirs();
        String jmhFile = null;
        try {
            jmhFile = String.valueOf(Files.list(Paths.get(jmhDir))
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
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        return String.format("dir=%s/%s;output=text;output=flamegraph", jmhDir, jmhFile);
    }
}
