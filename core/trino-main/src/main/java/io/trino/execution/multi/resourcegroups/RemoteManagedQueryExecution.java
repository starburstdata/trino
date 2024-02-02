package io.trino.execution.multi.resourcegroups;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.SystemSessionProperties;
import io.trino.execution.ManagedQueryExecution;
import io.trino.execution.QueryExecution;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryState;
import io.trino.execution.StageId;
import io.trino.execution.StateMachine;
import io.trino.execution.TaskId;
import io.trino.memory.LowMemoryKiller;
import io.trino.memory.LowMemoryKiller.RunningTaskInfo;
import io.trino.metadata.InternalNode;
import io.trino.server.BasicQueryInfo;
import io.trino.server.protocol.Slug;
import io.trino.spi.ErrorCode;
import io.trino.spi.QueryId;
import io.trino.sql.planner.Plan;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

public class RemoteManagedQueryExecution
        implements ManagedQueryExecution, QueryExecution
{
    private final ResourceGroupEvaluationSecondaryClient resourceGroupEvaluationSecondaryClient;
    private final InternalNode coordinator;
    private final QueryId queryId;

    private volatile RemoteQueryState remoteQueryState;
    private final List<Runnable> doneCallbacks = new ArrayList<>();
    private final Object stateLock = new Object();
    private final Session session;

    public RemoteManagedQueryExecution(
            ResourceGroupEvaluationSecondaryClient resourceGroupEvaluationSecondaryClient,
            InternalNode coordinator,
            QueryId queryId,
            Session session)
    {
        this.resourceGroupEvaluationSecondaryClient = requireNonNull(resourceGroupEvaluationSecondaryClient, "resourceGroupEvaluationSecondaryClient is null");
        this.coordinator = requireNonNull(coordinator, "coordinator is null");
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.session = session;
        this.remoteQueryState = new RemoteQueryState(QueryState.DISPATCHING, Duration.ZERO, DataSize.ofBytes(0), DataSize.ofBytes(0), DateTime.now(), ImmutableMap.of());
    }

    @Override
    public void startWaitingForResources()
    {
        resourceGroupEvaluationSecondaryClient.startWaitingForResources(coordinator, queryId);
    }

    @Override
    public void fail(Throwable cause)
    {
        resourceGroupEvaluationSecondaryClient.fail(coordinator, queryId, cause);
    }

    @Override
    public void pruneInfo()
    {
        throw new UnsupportedOperationException();
    }

    public QueryId getQueryId()
    {
        return queryId;
    }

    public InternalNode getCoordinator()
    {
        return coordinator;
    }

    @Override
    public int getQueryPriority()
    {
        return SystemSessionProperties.getQueryPriority(session);
    }

    @Override
    public DataSize getTotalMemoryReservation()
    {
        return remoteQueryState.totalMemoryReservation;
    }

    @Override
    public Map<TaskId, RunningTaskInfo> getTaskInfo()
    {
        return remoteQueryState.taskInfo();
    }

    @Override
    public void start()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void cancelQuery()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void cancelStage(StageId stageId)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void failTask(TaskId taskId, Exception reason)
    {
        resourceGroupEvaluationSecondaryClient.failTask(coordinator, taskId, reason);
    }

    @Override
    public void recordHeartbeat()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean shouldWaitForMinWorkers()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addFinalQueryInfoListener(StateMachine.StateChangeListener<QueryInfo> stateChangeListener)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Duration getTotalCpuTime()
    {
        return remoteQueryState.totalCpuTime;
    }

    @Override
    public DataSize getUserMemoryReservation()
    {
        return remoteQueryState.userMemoryReservation;
    }

    @Override
    public QueryState getState()
    {
        return remoteQueryState.state;
    }

    @Override
    public ListenableFuture<QueryState> getStateChange(QueryState currentState)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isDone()
    {
        return getState().isDone();
    }

    @Override
    public Session getSession()
    {
        return session;
    }

    @Override
    public DateTime getCreateTime()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<DateTime> getExecutionStartTime()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<Duration> getPlanningTime()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public DateTime getLastHeartbeat()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<DateTime> getEndTime()
    {
        throw new UnsupportedOperationException();
    }

    public void setRemoteQueryState(RemoteQueryState newState)
    {
        synchronized (stateLock) {
            remoteQueryState = newState;
            if (remoteQueryState.state.isDone()) {
                doneCallbacks.forEach(Runnable::run);
                doneCallbacks.clear();
            }
        }
    }

    @Override
    public void addDoneCallback(Runnable callback)
    {
        synchronized (stateLock) {
            if (remoteQueryState.state.isDone()) {
                callback.run();
            }
            doneCallbacks.add(callback);
        }
    }

    @Override
    public void addStateChangeListener(StateMachine.StateChangeListener<QueryState> stateChangeListener)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setOutputInfoListener(Consumer<QueryOutputInfo> listener)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void outputTaskFailed(TaskId taskId, Throwable failure)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void resultsConsumed()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Plan getQueryPlan()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public BasicQueryInfo getBasicQueryInfo()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryInfo getQueryInfo()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Slug getSlug()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryInfo getFullQueryInfo()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<ErrorCode> getErrorCode()
    {
        throw new UnsupportedOperationException();
    }

    public record RemoteQueryState(
            QueryState state,
            Duration totalCpuTime,
            DataSize totalMemoryReservation,
            DataSize userMemoryReservation,
            DateTime updatedAt,
            Map<TaskId, RunningTaskInfo> taskInfo)
    {
        public RemoteQueryState
        {
            requireNonNull(state, "state is null");
            requireNonNull(totalCpuTime, "totalCpuTime is null");
            requireNonNull(totalMemoryReservation, "totalMemoryReservation is null");
            requireNonNull(userMemoryReservation, "userMemoryReservation is null");
            requireNonNull(updatedAt, "updatedAt is null");
            requireNonNull(taskInfo, "taskInfo is null");
        }
    }
}
