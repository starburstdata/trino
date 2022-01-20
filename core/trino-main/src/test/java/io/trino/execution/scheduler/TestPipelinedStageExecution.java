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
package io.trino.execution.scheduler;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import io.trino.client.NodeVersion;
import io.trino.connector.CatalogName;
import io.trino.cost.StatsAndCosts;
import io.trino.execution.Lifespan;
import io.trino.execution.MockRemoteTaskFactory;
import io.trino.execution.MockRemoteTaskFactory.MockRemoteTask;
import io.trino.execution.NodeTaskMap;
import io.trino.execution.SqlStage;
import io.trino.execution.StageId;
import io.trino.execution.TableInfo;
import io.trino.failuredetector.NoOpFailureDetector;
import io.trino.metadata.InMemoryNodeManager;
import io.trino.metadata.InternalNode;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.Split;
import io.trino.spi.QueryId;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.planner.Partitioning;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.TestingMetadata;
import io.trino.testing.TestingSplit;
import io.trino.util.FinalizerService;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.execution.scheduler.PipelinedStageExecution.createPipelinedStageExecution;
import static io.trino.execution.scheduler.StageExecution.State.FINISHED;
import static io.trino.execution.scheduler.StageExecution.State.FINISHING;
import static io.trino.execution.scheduler.StageExecution.State.PLANNED;
import static io.trino.execution.scheduler.StageExecution.State.SCHEDULING;
import static io.trino.operator.StageExecutionDescriptor.ungroupedExecution;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static io.trino.testing.TestingHandles.TEST_TABLE_HANDLE;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPipelinedStageExecution
{
    private static final PlanNodeId TABLE_SCAN_NODE_ID = new PlanNodeId("plan_id");
    private static final CatalogName CONNECTOR_ID = TEST_TABLE_HANDLE.getCatalogName();
    private static final QueryId QUERY_ID = new QueryId("query");
    private static final InternalNode NODE = new InternalNode("other1", URI.create("http://127.0.0.1:11"), NodeVersion.UNKNOWN, false);

    private final ExecutorService queryExecutor = newCachedThreadPool(daemonThreadsNamed("stageExecutor-%s"));
    private final ScheduledExecutorService scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("stageScheduledExecutor-%s"));
    private final InMemoryNodeManager nodeManager = new InMemoryNodeManager();
    private final FinalizerService finalizerService = new FinalizerService();

    public TestPipelinedStageExecution()
    {
        nodeManager.addNode(CONNECTOR_ID, NODE);
    }

    @BeforeClass
    public void setUp()
    {
        finalizerService.start();
    }

    @AfterClass(alwaysRun = true)
    public void destroyExecutor()
    {
        queryExecutor.shutdownNow();
        scheduledExecutor.shutdownNow();
        finalizerService.destroy();
    }

    @Test
    public void testStageStates()
    {
        PlanFragment plan = createFragment();
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);
        StageExecution stage = createStageExecution(plan, nodeTaskMap);

        assertThat(stage.getState()).isEqualTo(PLANNED);

        stage.beginScheduling();
        assertThat(stage.getState()).isEqualTo(SCHEDULING);

        MockRemoteTask task = (MockRemoteTask) stage.scheduleTask(NODE, 0, ImmutableMultimap.of(), ImmutableMultimap.of()).get();

        Split split = new Split(new CatalogName("test"), TestingSplit.createLocalSplit(), Lifespan.taskWide());
        task.addSplits(ImmutableMultimap.of(TABLE_SCAN_NODE_ID, split));
        task.startSplits(1);

        assertThat(stage.getState()).isEqualTo(SCHEDULING);

        // task should receive "no more splits" signal and enter finishing state as splits are still running
        stage.schedulingComplete();
        assertEventually(() -> assertThat(stage.getState()).isEqualTo(FINISHING));

        task.finishSplits(1);
        assertEventually(() -> assertThat(stage.getState()).isEqualTo(FINISHED));
    }

    private StageExecution createStageExecution(PlanFragment fragment, NodeTaskMap nodeTaskMap)
    {
        StageId stageId = new StageId(QUERY_ID, 0);
        SqlStage stage = SqlStage.createSqlStage(stageId,
                fragment,
                ImmutableMap.of(TABLE_SCAN_NODE_ID, new TableInfo(new QualifiedObjectName("test", "test", "test"), TupleDomain.all())),
                new MockRemoteTaskFactory(queryExecutor, scheduledExecutor),
                TEST_SESSION,
                true,
                nodeTaskMap,
                queryExecutor,
                new SplitSchedulerStats());
        ImmutableMap.Builder<PlanFragmentId, OutputBufferManager> outputBuffers = ImmutableMap.builder();
        outputBuffers.put(fragment.getId(), new PartitionedOutputBufferManager(FIXED_HASH_DISTRIBUTION, 1));
        fragment.getRemoteSourceNodes().stream()
                .flatMap(node -> node.getSourceFragmentIds().stream())
                .forEach(fragmentId -> outputBuffers.put(fragmentId, new PartitionedOutputBufferManager(FIXED_HASH_DISTRIBUTION, 10)));
        return createPipelinedStageExecution(
                stage,
                outputBuffers.build(),
                TaskLifecycleListener.NO_OP,
                new NoOpFailureDetector(),
                queryExecutor,
                Optional.of(new int[] {0}),
                0);
    }

    private static PlanFragment createFragment()
    {
        Symbol symbol = new Symbol("column");

        TableScanNode tableScan = TableScanNode.newInstance(
                TABLE_SCAN_NODE_ID,
                TEST_TABLE_HANDLE,
                ImmutableList.of(symbol),
                ImmutableMap.of(symbol, new TestingMetadata.TestingColumnHandle("column")),
                false,
                Optional.empty());

        return new PlanFragment(
                new PlanFragmentId("plan_id"),
                tableScan,
                ImmutableMap.of(symbol, VARCHAR),
                SOURCE_DISTRIBUTION,
                ImmutableList.of(TABLE_SCAN_NODE_ID),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(symbol)),
                ungroupedExecution(),
                StatsAndCosts.empty(),
                Optional.empty());
    }
}
